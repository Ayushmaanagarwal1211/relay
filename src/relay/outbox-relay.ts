import { Pool } from 'pg';
import { withTransaction } from '../shared/db';

// Something that can publish messages and confirm they were received.
// In production this is RabbitMQ; in tests it's a fake, so the relay logic can be tested alone.
export interface Publisher {
  publish(routingKey: string, payload: object, messageId: string): void;
  // Resolves once the broker has confirmed every message published so far.
  waitForConfirms(): Promise<void>;
}

interface OutboxRow {
  id: string;
  email_id: string;
  routing_key: string;
  payload: object;
}

/**
 * Publishes up to `batchSize` unpublished outbox rows, then marks them published.
 * Returns how many rows it published.
 *
 * The order matters:
 *   1. lock the rows
 *   2. publish them and WAIT for RabbitMQ to confirm
 *   3. mark them published and commit
 *
 * Crash before step 3 commits? The transaction rolls back, the rows stay unpublished,
 * and the next run publishes them again. That's a duplicate message, never a lost one.
 * The worker skips emails that are already sent, so the duplicate is harmless.
 */
export async function relayBatch(pool: Pool, publisher: Publisher, batchSize: number): Promise<number> {
  return withTransaction(pool, async (client) => {
    // FOR UPDATE SKIP LOCKED: lock these rows, and skip rows another relay already locked.
    // That lets you run several relays at once without two of them publishing the same row.
    const { rows } = await client.query<OutboxRow>(
      `SELECT id, email_id, routing_key, payload
         FROM outbox
        WHERE published_at IS NULL
        ORDER BY id
        LIMIT $1
        FOR UPDATE SKIP LOCKED`,
      [batchSize],
    );
    if (rows.length === 0) return 0;

    for (const row of rows) {
      publisher.publish(row.routing_key, row.payload, row.email_id);
    }
    // If RabbitMQ rejects or loses any message, this throws and the transaction rolls back.
    await publisher.waitForConfirms();

    await client.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1)', [rows.map((r) => r.id)]);
    return rows.length;
  });
}
