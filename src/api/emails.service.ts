import { Pool } from 'pg';
import { hashRequest } from './request-hash';
import type { CreateEmailInput } from './schemas';

export interface EmailRow {
  id: string;
  idempotency_key: string;
  request_hash: string;
  to_address: string;
  subject: string;
  body: string;
  priority: 'transactional' | 'promotional';
  status: 'queued' | 'sending' | 'sent' | 'failed';
  attempts: number;
  last_error: string | null;
  created_at: Date;
  updated_at: Date;
  sent_at: Date | null;
}

export type CreateEmailResult =
  | { kind: 'created'; email: EmailRow }
  | { kind: 'replayed'; email: EmailRow } // same key, same body: a client retry
  | { kind: 'key_reused' }; // same key, different body: a client bug

export async function createEmail(
  pool: Pool,
  idempotencyKey: string,
  input: CreateEmailInput,
): Promise<CreateEmailResult> {
  const requestHash = hashRequest(input);

  // ONE statement saves the email row AND its outbox row. A single statement is atomic on
  // its own: both rows are saved, or neither is, with no BEGIN/COMMIT needed.
  // (The first version ran BEGIN, INSERT email, INSERT outbox, COMMIT: 4 round trips to
  // Postgres per request. This is 1. The stress test showed the difference.)
  //
  // ON CONFLICT DO NOTHING: if this key already exists, insert nothing and return no rows,
  // so the outbox insert (which reads new_email) inserts nothing too. If another request
  // with the same key is mid-insert, Postgres makes this one wait until it finishes,
  // so exactly one wins.
  const inserted = await pool.query<EmailRow>(
    `WITH new_email AS (
       INSERT INTO emails (idempotency_key, request_hash, to_address, subject, body, priority)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (idempotency_key) DO NOTHING
       RETURNING *
     ), new_outbox AS (
       -- The queue message carries only the id; the worker reads the rest from Postgres.
       INSERT INTO outbox (email_id, routing_key, payload)
       SELECT id, priority, jsonb_build_object('emailId', id) FROM new_email
     )
     SELECT * FROM new_email`,
    [idempotencyKey, requestHash, input.to, input.subject, input.body, input.priority],
  );
  if (inserted.rows.length === 1) return { kind: 'created', email: inserted.rows[0] };

  // The key was used before. Return the original email if the body matches.
  const existing = await pool.query<EmailRow>('SELECT * FROM emails WHERE idempotency_key = $1', [idempotencyKey]);
  const email = existing.rows[0];
  if (email.request_hash !== requestHash) return { kind: 'key_reused' };
  return { kind: 'replayed', email };
}

export async function getEmail(pool: Pool, id: string): Promise<EmailRow | null> {
  const { rows } = await pool.query<EmailRow>('SELECT * FROM emails WHERE id = $1', [id]);
  return rows[0] ?? null;
}

// What clients see. Internal fields (idempotency key, hash, body) stay private.
export function toEmailResponse(email: EmailRow) {
  return {
    id: email.id,
    to: email.to_address,
    subject: email.subject,
    priority: email.priority,
    status: email.status,
    attempts: email.attempts,
    lastError: email.last_error,
    createdAt: email.created_at,
    sentAt: email.sent_at,
  };
}
