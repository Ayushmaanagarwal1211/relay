import { randomUUID } from 'node:crypto';
import { createEmail } from '../../src/api/emails.service';
import { createPool } from '../../src/shared/db';
import { processEmail, ProcessDeps } from '../../src/worker/process-email';
import { FakeProvider } from '../helpers/fake-provider';
import { testDatabaseUrl } from './test-db';

const pool = createPool(testDatabaseUrl());
const delays = [10_000, 60_000, 300_000];

function deps(provider: FakeProvider, sendingLeaseMs = 60_000): ProcessDeps {
  return { pool, provider, retryDelaysMs: delays, sendingLeaseMs };
}

async function newEmail(): Promise<string> {
  const result = await createEmail(pool, randomUUID(), {
    to: 'user@example.com',
    subject: 'Your OTP',
    body: 'Code: 123456',
    priority: 'transactional',
  });
  if (result.kind !== 'created') throw new Error('expected a new email');
  return result.email.id;
}

async function emailRow(id: string) {
  const { rows } = await pool.query('SELECT status, attempts, last_error, sent_at FROM emails WHERE id = $1', [id]);
  return rows[0];
}

beforeEach(async () => {
  await pool.query('TRUNCATE outbox, emails');
});

afterAll(async () => {
  await pool.end();
});

describe('processEmail', () => {
  it('sends a queued email and marks it sent', async () => {
    const id = await newEmail();
    const provider = new FakeProvider(['ok']);

    expect(await processEmail(deps(provider), id)).toEqual({ outcome: 'sent', attempts: 1 });
    expect(provider.sent).toHaveLength(1);
    expect(await emailRow(id)).toMatchObject({ status: 'sent', attempts: 1, last_error: null });
  });

  it('skips an email that was already sent (duplicate message), without sending again', async () => {
    const id = await newEmail();
    const provider = new FakeProvider(['ok']);
    await processEmail(deps(provider), id);

    expect(await processEmail(deps(provider), id)).toEqual({ outcome: 'skipped', reason: 'already-sent' });
    expect(provider.sent).toHaveLength(1);
  });

  it('two workers handling the same message at once send the email only once', async () => {
    const id = await newEmail();
    const provider = new FakeProvider(['ok']);

    const results = await Promise.all([processEmail(deps(provider), id), processEmail(deps(provider), id)]);

    expect(provider.sent).toHaveLength(1);
    expect(results.filter((r) => r.outcome === 'sent')).toHaveLength(1);
  });

  it('on a temporary failure, puts the email back to queued and asks for a retry', async () => {
    const id = await newEmail();
    const result = await processEmail(deps(new FakeProvider(['fail'])), id);

    expect(result).toMatchObject({ outcome: 'retry', delayMs: 10_000, attempts: 1 });
    expect(await emailRow(id)).toMatchObject({
      status: 'queued',
      attempts: 1,
      last_error: 'provider temporarily unavailable',
    });
  });

  it('succeeds on a later attempt after temporary failures', async () => {
    const id = await newEmail();
    const provider = new FakeProvider(['fail', 'fail', 'ok']);

    expect((await processEmail(deps(provider), id)).outcome).toBe('retry');
    expect((await processEmail(deps(provider), id)).outcome).toBe('retry');
    expect(await processEmail(deps(provider), id)).toEqual({ outcome: 'sent', attempts: 3 });
    expect(await emailRow(id)).toMatchObject({ status: 'sent', attempts: 3, last_error: null });
  });

  it('gives up after the last retry and marks the email failed', async () => {
    const id = await newEmail();
    const provider = new FakeProvider(['fail']);

    for (let attempt = 1; attempt <= delays.length; attempt++) {
      expect((await processEmail(deps(provider), id)).outcome).toBe('retry');
    }
    expect(await processEmail(deps(provider), id)).toMatchObject({
      outcome: 'dead-letter',
      reason: 'max-attempts',
      attempts: 4,
    });
    expect(await emailRow(id)).toMatchObject({ status: 'failed', attempts: 4 });
  });

  it('gives up straight away on a permanent error', async () => {
    const id = await newEmail();
    const result = await processEmail(deps(new FakeProvider(['permanent'])), id);

    expect(result).toMatchObject({ outcome: 'dead-letter', reason: 'permanent-error', attempts: 1 });
    expect(await emailRow(id)).toMatchObject({ status: 'failed', last_error: 'mailbox does not exist' });
  });

  it('reports busy while another worker holds a fresh lease', async () => {
    const id = await newEmail();
    await pool.query(`UPDATE emails SET status = 'sending', updated_at = now() WHERE id = $1`, [id]);

    expect(await processEmail(deps(new FakeProvider()), id)).toEqual({ outcome: 'busy' });
  });

  it('takes over an email whose worker crashed mid-send (lease expired)', async () => {
    const id = await newEmail();
    // Simulate a worker that claimed the email 2 minutes ago and then died.
    await pool.query(
      `UPDATE emails SET status = 'sending', attempts = 1, updated_at = now() - interval '2 minutes' WHERE id = $1`,
      [id],
    );
    const provider = new FakeProvider(['ok']);

    expect(await processEmail(deps(provider, 60_000), id)).toEqual({ outcome: 'sent', attempts: 2 });
    expect(provider.sent).toHaveLength(1);
  });

  it('skips a message for an email that does not exist', async () => {
    expect(await processEmail(deps(new FakeProvider()), randomUUID())).toEqual({
      outcome: 'skipped',
      reason: 'not-found',
    });
  });
});
