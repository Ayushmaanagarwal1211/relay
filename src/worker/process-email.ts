import { Pool } from 'pg';
import type { EmailRow } from '../api/emails.service';
import { EmailProvider, PermanentSendError } from './email-provider';
import { decideOnFailure } from './retry-policy';

export interface ProcessDeps {
  pool: Pool;
  provider: EmailProvider;
  retryDelaysMs: number[];
  sendingLeaseMs: number;
}

export type ProcessResult =
  | { outcome: 'sent'; attempts: number }
  | { outcome: 'skipped'; reason: 'not-found' | 'already-sent' | 'already-failed' }
  | { outcome: 'busy' } // another worker is sending this email right now
  | { outcome: 'retry'; delayMs: number; attempts: number; error: string }
  | { outcome: 'dead-letter'; reason: 'permanent-error' | 'max-attempts'; attempts: number; error: string };

/**
 * Sends one email and records the result in Postgres. Knows nothing about RabbitMQ:
 * it returns what happened, and the consumer decides what to do with the message.
 *
 * Status changes:
 *   queued ──claim──► sending ──success──► sent
 *                        │
 *                        └──failure──► queued (retry later) or failed (gave up)
 */
export async function processEmail(deps: ProcessDeps, emailId: string): Promise<ProcessResult> {
  const { pool, provider, retryDelaysMs, sendingLeaseMs } = deps;

  // 1. CLAIM the email: atomically move it to 'sending' and count the attempt.
  //    The WHERE clause is the idempotency check. If the email is already sent or failed,
  //    or another worker is sending it, no row matches and we don't send it again.
  //    A 'sending' email whose lease expired belongs to a worker that crashed mid-send,
  //    so we take it over.
  const claimed = await pool.query<EmailRow>(
    `UPDATE emails
        SET status = 'sending', attempts = attempts + 1, updated_at = now()
      WHERE id = $1
        AND (status = 'queued'
             OR (status = 'sending' AND updated_at < now() - make_interval(secs => $2)))
      RETURNING *`,
    [emailId, sendingLeaseMs / 1000],
  );

  if (claimed.rows.length === 0) {
    const { rows } = await pool.query<{ status: EmailRow['status'] }>('SELECT status FROM emails WHERE id = $1', [
      emailId,
    ]);
    if (rows.length === 0) return { outcome: 'skipped', reason: 'not-found' };
    if (rows[0].status === 'sent') return { outcome: 'skipped', reason: 'already-sent' };
    if (rows[0].status === 'failed') return { outcome: 'skipped', reason: 'already-failed' };
    return { outcome: 'busy' };
  }

  const email = claimed.rows[0];

  // 2. SEND. A crash between here and step 3 is the one unavoidable duplicate:
  //    the email went out, but we never recorded it. After the lease expires,
  //    another worker sends it again. That's what "at-least-once" means.
  try {
    await provider.send({ id: email.id, to: email.to_address, subject: email.subject, body: email.body });
  } catch (err) {
    // 3a. FAILED: decide whether to retry, and record the error.
    const error = err instanceof Error ? err.message : String(err);
    const decision = decideOnFailure(email.attempts, retryDelaysMs, err instanceof PermanentSendError);
    const nextStatus = decision.action === 'retry' ? 'queued' : 'failed';
    await pool.query(`UPDATE emails SET status = $2, last_error = $3, updated_at = now() WHERE id = $1`, [
      email.id,
      nextStatus,
      error,
    ]);
    return decision.action === 'retry'
      ? { outcome: 'retry', delayMs: decision.delayMs, attempts: email.attempts, error }
      : { outcome: 'dead-letter', reason: decision.reason, attempts: email.attempts, error };
  }

  // 3b. SENT: record success.
  await pool.query(
    `UPDATE emails SET status = 'sent', sent_at = now(), last_error = NULL, updated_at = now() WHERE id = $1`,
    [email.id],
  );
  return { outcome: 'sent', attempts: email.attempts };
}
