import { randomUUID } from 'node:crypto';
import request from 'supertest';
import { createApp } from '../../src/api/app';
import { createPool } from '../../src/shared/db';
import { testDatabaseUrl } from './test-db';

const pool = createPool(testDatabaseUrl());
const app = createApp(pool);

const email = { to: 'user@example.com', subject: 'Your OTP', body: 'Your code is 123456' };

const send = (key: string, body: object = email) =>
  request(app).post('/v1/emails').set('Idempotency-Key', key).send(body);

async function count(table: 'emails' | 'outbox'): Promise<number> {
  const { rows } = await pool.query(`SELECT count(*)::int AS n FROM ${table}`);
  return rows[0].n;
}

beforeEach(async () => {
  await pool.query('TRUNCATE outbox, emails');
});

afterAll(async () => {
  await pool.end();
});

describe('POST /v1/emails', () => {
  it('queues a new email and writes one outbox row in the same transaction', async () => {
    const res = await send(randomUUID());

    expect(res.status).toBe(202);
    expect(res.body).toMatchObject({ to: email.to, status: 'queued', priority: 'transactional' });
    expect(res.headers['idempotent-replayed']).toBeUndefined();

    const { rows } = await pool.query('SELECT email_id, routing_key, payload FROM outbox');
    expect(rows).toEqual([
      { email_id: res.body.id, routing_key: 'transactional', payload: { emailId: res.body.id } },
    ]);
  });

  it('returns the original email when the client retries with the same key', async () => {
    const key = randomUUID();
    const first = await send(key);
    const retry = await send(key);

    expect(retry.status).toBe(202);
    expect(retry.headers['idempotent-replayed']).toBe('true');
    expect(retry.body.id).toBe(first.body.id);
    expect(await count('emails')).toBe(1);
    expect(await count('outbox')).toBe(1);
  });

  it('rejects a reused key with a different body', async () => {
    const key = randomUUID();
    await send(key);
    const res = await send(key, { ...email, body: 'A different email' });

    expect(res.status).toBe(422);
    expect(await count('emails')).toBe(1);
  });

  it('creates exactly one email when 20 identical requests arrive at the same time', async () => {
    const key = randomUUID();
    const responses = await Promise.all(Array.from({ length: 20 }, () => send(key)));

    const ids = new Set(responses.map((r) => r.body.id));
    expect(responses.every((r) => r.status === 202)).toBe(true);
    expect(ids.size).toBe(1);
    expect(responses.filter((r) => r.headers['idempotent-replayed'] !== 'true')).toHaveLength(1);
    expect(await count('emails')).toBe(1);
    expect(await count('outbox')).toBe(1);
  });

  it('returns 400 without an Idempotency-Key header', async () => {
    const res = await request(app).post('/v1/emails').send(email);
    expect(res.status).toBe(400);
  });

  it('returns 400 with field errors for an invalid body', async () => {
    const res = await send(randomUUID(), { ...email, to: 'not-an-email' });
    expect(res.status).toBe(400);
    expect(res.body.details).toHaveProperty('to');
  });

  it('returns 400 for malformed JSON', async () => {
    const res = await request(app)
      .post('/v1/emails')
      .set('Idempotency-Key', randomUUID())
      .set('Content-Type', 'application/json')
      .send('{"to": ');
    expect(res.status).toBe(400);
  });
});

describe('GET /v1/emails/:id', () => {
  it('returns the email status', async () => {
    const created = await send(randomUUID());
    const res = await request(app).get(`/v1/emails/${created.body.id}`);

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ id: created.body.id, status: 'queued', attempts: 0 });
  });

  it('returns 404 for an unknown or malformed id', async () => {
    expect((await request(app).get(`/v1/emails/${randomUUID()}`)).status).toBe(404);
    expect((await request(app).get('/v1/emails/not-a-uuid')).status).toBe(404);
  });
});
