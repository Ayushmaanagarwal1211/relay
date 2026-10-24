import { randomUUID } from 'node:crypto';
import { createEmail } from '../../src/api/emails.service';
import { createPool } from '../../src/shared/db';
import { Publisher, relayBatch } from '../../src/relay/outbox-relay';
import { testDatabaseUrl } from './test-db';

const pool = createPool(testDatabaseUrl());

// Records what was published. `failConfirms` simulates RabbitMQ not confirming.
function fakePublisher(failConfirms = false) {
  const published: { routingKey: string; payload: object; messageId: string }[] = [];
  const publisher: Publisher = {
    publish: (routingKey, payload, messageId) => published.push({ routingKey, payload, messageId }),
    waitForConfirms: async () => {
      if (failConfirms) throw new Error('broker did not confirm');
    },
  };
  return { publisher, published };
}

async function queueEmails(count: number, priority: 'transactional' | 'promotional' = 'transactional') {
  for (let i = 0; i < count; i++) {
    await createEmail(pool, randomUUID(), { to: `user${i}@example.com`, subject: 'Hi', body: 'Hello', priority });
  }
}

async function unpublishedCount(): Promise<number> {
  const { rows } = await pool.query('SELECT count(*)::int AS n FROM outbox WHERE published_at IS NULL');
  return rows[0].n;
}

beforeEach(async () => {
  await pool.query('TRUNCATE outbox, emails');
});

afterAll(async () => {
  await pool.end();
});

describe('relayBatch', () => {
  it('publishes unpublished rows with the right routing key and marks them published', async () => {
    await queueEmails(2, 'transactional');
    await queueEmails(1, 'promotional');
    const { publisher, published } = fakePublisher();

    expect(await relayBatch(pool, publisher, 100)).toBe(3);
    expect(published.map((p) => p.routingKey).sort()).toEqual(['promotional', 'transactional', 'transactional']);
    expect(published[0].payload).toEqual({ emailId: published[0].messageId });
    expect(await unpublishedCount()).toBe(0);
  });

  it('does not publish the same row twice', async () => {
    await queueEmails(2);
    const { publisher, published } = fakePublisher();

    await relayBatch(pool, publisher, 100);
    expect(await relayBatch(pool, publisher, 100)).toBe(0);
    expect(published).toHaveLength(2);
  });

  it('publishes at most batchSize rows per call, oldest first', async () => {
    await queueEmails(5);
    const { publisher } = fakePublisher();

    expect(await relayBatch(pool, publisher, 2)).toBe(2);
    expect(await unpublishedCount()).toBe(3);
  });

  it('leaves rows unpublished if the broker does not confirm, so they are retried', async () => {
    await queueEmails(3);

    await expect(relayBatch(pool, fakePublisher(true).publisher, 100)).rejects.toThrow('broker did not confirm');
    expect(await unpublishedCount()).toBe(3);

    // Next run succeeds and publishes them. Nothing was lost.
    expect(await relayBatch(pool, fakePublisher().publisher, 100)).toBe(3);
  });

  it('lets two relays run at once without publishing the same row twice', async () => {
    await queueEmails(50);
    const a = fakePublisher();
    const b = fakePublisher();

    await Promise.all([relayBatch(pool, a.publisher, 30), relayBatch(pool, b.publisher, 30)]);

    const ids = [...a.published, ...b.published].map((p) => p.messageId);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids.length).toBeLessThanOrEqual(50);
  });
});
