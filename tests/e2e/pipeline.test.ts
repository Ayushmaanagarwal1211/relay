/**
 * End-to-end: API → Postgres outbox → relay → RabbitMQ → worker → email provider.
 * Needs Postgres, RabbitMQ and Mailpit running (npm run infra:up).
 *
 * Each test gets its own exchange and queues (a random prefix), and is deleted afterwards,
 * so tests never see each other's messages.
 */
import { randomUUID } from 'node:crypto';
import { ChannelModel } from 'amqplib';
import request from 'supertest';
import { createApp } from '../../src/api/app';
import { createPool } from '../../src/shared/db';
import { connectRabbit, createTopology, PRIORITIES, Topology } from '../../src/shared/rabbitmq';
import { startRelay } from '../../src/relay/run-relay';
import { createRedis, RateLimiter, waitForRedis } from '../../src/shared/rate-limiter';
import Redis from 'ioredis';
import { EmailProvider, SmtpProvider } from '../../src/worker/email-provider';
import { startWorker } from '../../src/worker/run-worker';
import { FakeProvider } from '../helpers/fake-provider';
import { testDatabaseUrl } from '../integration/test-db';

jest.setTimeout(30_000);

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://relay:relay@localhost:5672';
const SMTP_HOST = process.env.SMTP_HOST ?? 'localhost';
const SMTP_PORT = Number(process.env.SMTP_PORT ?? 1025);
const MAILPIT_URL = process.env.MAILPIT_URL ?? 'http://localhost:8025';
const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';

const pool = createPool(testDatabaseUrl());
const app = createApp(pool);
let connection: ChannelModel;
let redis: Redis;
let stopFns: (() => Promise<void>)[] = [];
let topology: Topology;

beforeAll(async () => {
  connection = await connectRabbit(RABBITMQ_URL);
  redis = createRedis(REDIS_URL);
  await waitForRedis(redis);
});

beforeEach(async () => {
  await pool.query('TRUNCATE outbox, emails');
});

afterEach(async () => {
  for (const stop of stopFns.reverse()) await stop();
  stopFns = [];
  if (topology) await deleteTopology(topology);
});

afterAll(async () => {
  redis.disconnect();
  await connection.close();
  await pool.end();
});

// Starts a relay and a worker with short retry delays, on a fresh isolated topology.
async function startPipeline(
  provider: EmailProvider,
  retryDelaysMs = [200, 200],
  throttle?: RateLimiter,
) {
  topology = createTopology(retryDelaysMs, `test-${randomUUID().slice(0, 8)}`);
  const relay = await startRelay({ pool, connection, topology, pollIntervalMs: 50, batchSize: 100 });
  const worker = await startWorker({
    pool,
    connection,
    topology,
    provider,
    priorities: PRIORITIES,
    prefetch: 10,
    sendingLeaseMs: 60_000,
    throttle,
  });
  stopFns.push(relay.stop, worker.stop);
}

async function deleteTopology(t: Topology) {
  const channel = await connection.createChannel();
  for (const priority of PRIORITIES) {
    await channel.deleteQueue(t.mainQueue(priority));
    for (const delay of t.retryDelaysMs) await channel.deleteQueue(t.retryQueue(priority, delay));
  }
  await channel.deleteQueue(t.deadLetterQueue);
  await channel.deleteExchange(t.exchange);
  await channel.close();
}

async function postEmail(body: object) {
  const res = await request(app).post('/v1/emails').set('Idempotency-Key', randomUUID()).send(body);
  expect(res.status).toBe(202);
  return res.body.id as string;
}

// Polls until the email reaches a final status (sent or failed).
async function waitForFinalStatus(id: string, timeoutMs = 15_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const { rows } = await pool.query('SELECT status, attempts, last_error FROM emails WHERE id = $1', [id]);
    if (rows[0] && ['sent', 'failed'].includes(rows[0].status)) return rows[0];
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`email ${id} did not finish within ${timeoutMs}ms`);
}

async function deadLetterMessages(): Promise<{ emailId: string; headers: Record<string, unknown> }[]> {
  const channel = await connection.createChannel();
  const messages = [];
  for (let msg = await channel.get(topology.deadLetterQueue); msg; msg = await channel.get(topology.deadLetterQueue)) {
    messages.push({ emailId: JSON.parse(msg.content.toString()).emailId, headers: msg.properties.headers ?? {} });
    channel.ack(msg);
  }
  await channel.close();
  return messages;
}

describe('email pipeline', () => {
  it('delivers an email through SMTP to Mailpit', async () => {
    await startPipeline(new SmtpProvider({ host: SMTP_HOST, port: SMTP_PORT }, 'Relay <no-reply@relay.local>'));
    const subject = `e2e ${randomUUID()}`;

    const id = await postEmail({ to: 'user@example.com', subject, body: 'Hello from the e2e test' });

    expect(await waitForFinalStatus(id)).toMatchObject({ status: 'sent', attempts: 1 });
    const inbox = await (await fetch(`${MAILPIT_URL}/api/v1/messages?limit=0`)).json();
    const delivered = inbox.messages.find((m: { Subject: string }) => m.Subject === subject);
    expect(delivered.To[0].Address).toBe('user@example.com');
  });

  it('delivers both priorities', async () => {
    const provider = new FakeProvider(['ok']);
    await startPipeline(provider);

    const otp = await postEmail({ to: 'a@example.com', subject: 'OTP', body: '1', priority: 'transactional' });
    const promo = await postEmail({ to: 'b@example.com', subject: 'Sale', body: '2', priority: 'promotional' });

    expect(await waitForFinalStatus(otp)).toMatchObject({ status: 'sent' });
    expect(await waitForFinalStatus(promo)).toMatchObject({ status: 'sent' });
  });

  it('retries through the delay queues and succeeds on the third attempt', async () => {
    const provider = new FakeProvider(['fail', 'fail', 'ok']);
    await startPipeline(provider, [200, 200]);

    const id = await postEmail({ to: 'user@example.com', subject: 'Retry me', body: 'Hello' });

    expect(await waitForFinalStatus(id)).toMatchObject({ status: 'sent', attempts: 3, last_error: null });
    expect(provider.sent).toHaveLength(1);
  });

  it('moves the email to the dead-letter queue after all retries fail', async () => {
    await startPipeline(new FakeProvider(['fail']), [200, 200]);

    const id = await postEmail({ to: 'user@example.com', subject: 'Doomed', body: 'Hello' });

    expect(await waitForFinalStatus(id)).toMatchObject({ status: 'failed', attempts: 3 });
    const dead = await deadLetterMessages();
    expect(dead).toHaveLength(1);
    expect(dead[0]).toMatchObject({ emailId: id, headers: { 'x-reason': 'max-attempts', 'x-attempts': 3 } });
  });

  it('dead-letters a permanent failure without retrying', async () => {
    const provider = new FakeProvider(['permanent']);
    await startPipeline(provider, [200, 200]);

    const id = await postEmail({ to: 'nobody@example.com', subject: 'Bounce', body: 'Hello' });

    expect(await waitForFinalStatus(id)).toMatchObject({ status: 'failed', attempts: 1 });
    expect(provider.calls).toBe(1);
    expect((await deadLetterMessages())[0].headers).toMatchObject({ 'x-reason': 'permanent-error' });
  });

  it('sends each email exactly once when the relay publishes a duplicate message', async () => {
    const provider = new FakeProvider(['ok']);
    await startPipeline(provider);
    const id = await postEmail({ to: 'user@example.com', subject: 'Once', body: 'Hello' });
    await waitForFinalStatus(id);

    // Simulate the relay crashing after publishing but before marking the row published:
    // the row becomes unpublished again and gets published a second time.
    await pool.query('UPDATE outbox SET published_at = NULL WHERE email_id = $1', [id]);
    await new Promise((r) => setTimeout(r, 1000));

    expect(provider.sent).toHaveLength(1);
  });

  it('spreads sends out to respect the provider rate limit', async () => {
    const provider = new FakeProvider(['ok']);
    // 10 emails/s, burst of 1: 8 emails need at least ~700ms
    const throttle = new RateLimiter(redis, { key: `test:ratelimit:${randomUUID()}`, ratePerSec: 10, capacity: 1 });
    await startPipeline(provider, [200, 200], throttle);

    const started = Date.now();
    const ids = await Promise.all(
      Array.from({ length: 8 }, (_, i) => postEmail({ to: `u${i}@example.com`, subject: 'Limited', body: 'Hi' })),
    );
    for (const id of ids) expect(await waitForFinalStatus(id)).toMatchObject({ status: 'sent' });

    expect(provider.sent).toHaveLength(8);
    expect(Date.now() - started).toBeGreaterThanOrEqual(650);
  });
});
