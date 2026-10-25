/**
 * One command for a full load test:  RATE=200 DURATION=30s npm run load:test
 *
 *   1. delete emails from previous load tests
 *   2. run k6 (in Docker, nothing to install) against the API
 *   3. wait until the workers have finished every email
 *   4. print a report: API speed, and how fast emails were actually delivered
 *
 * Start the app first, with the mock provider and quiet logs:
 *   EMAIL_PROVIDER=mock LOG_LEVEL=warn npm run dev
 */
import { appendFileSync } from 'node:fs';
import { config } from '../shared/config';
import { createPool } from '../shared/db';
import { runK6 } from './k6';

const RATE = process.env.RATE ?? '100';
const DURATION = process.env.DURATION ?? '30s';
const API_URL = process.env.API_URL ?? `http://localhost:${config.PORT}`;
const DRAIN_TIMEOUT_S = Number(process.env.DRAIN_TIMEOUT_S ?? 180);
const LOAD_TEST_EMAILS = `subject LIKE 'load-test %'`;

const pool = createPool(config.DATABASE_URL);

async function cleanPreviousRuns() {
  await pool.query(`DELETE FROM outbox WHERE email_id IN (SELECT id FROM emails WHERE ${LOAD_TEST_EMAILS})`);
  await pool.query(`DELETE FROM emails WHERE ${LOAD_TEST_EMAILS}`);
}

async function checkApiIsUp() {
  try {
    const res = await fetch(`${API_URL}/health`);
    if (res.ok) return;
  } catch {
    // fall through
  }
  console.error(`The API isn't running at ${API_URL}. Start the app first, in another terminal:`);
  console.error('  EMAIL_PROVIDER=mock LOG_LEVEL=warn npm run dev');
  process.exit(1);
}

async function waitForWorkers() {
  const deadline = Date.now() + DRAIN_TIMEOUT_S * 1000;
  while (Date.now() < deadline) {
    const { rows } = await pool.query(
      `SELECT count(*)::int AS pending FROM emails WHERE ${LOAD_TEST_EMAILS} AND status IN ('queued', 'sending')`,
    );
    if (rows[0].pending === 0) return;
    process.stdout.write(`\rWaiting for workers... ${rows[0].pending} email(s) still in progress   `);
    await new Promise((r) => setTimeout(r, 1000));
  }
  console.log(`\nStopped waiting after ${DRAIN_TIMEOUT_S}s; some emails are still in progress.`);
}

async function deliveryStats() {
  const { rows } = await pool.query(`
    SELECT count(*)::int                                        AS total,
           count(*) FILTER (WHERE status = 'sent')::int         AS sent,
           count(*) FILTER (WHERE status = 'failed')::int       AS failed,
           count(*) FILTER (WHERE status IN ('queued', 'sending'))::int AS pending,
           count(*) FILTER (WHERE attempts > 1)::int            AS retried,
           extract(epoch FROM max(sent_at) - min(created_at))   AS span_s,
           percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - created_at) * 1000)
             FILTER (WHERE status = 'sent') AS p50,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - created_at) * 1000)
             FILTER (WHERE status = 'sent') AS p95,
           percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM sent_at - created_at) * 1000)
             FILTER (WHERE status = 'sent') AS p99
      FROM emails
     WHERE ${LOAD_TEST_EMAILS}`);
  return rows[0];
}

const ms = (v: number | null) => (v == null ? '-' : `${Math.round(Number(v))} ms`);

async function main() {
  await checkApiIsUp();
  await cleanPreviousRuns();

  console.log(`Load test: ${RATE} requests/second for ${DURATION} against ${API_URL}\n`);
  const k6 = await runK6({ SCENARIO: 'steady', RATE, DURATION, API_URL });
  await waitForWorkers();
  const d = await deliveryStats();

  const spanS = Number(d.span_s) || 0;
  const report = [
    `## Load test: ${RATE} emails/second for ${DURATION}`,
    '',
    '**API (POST /v1/emails)**',
    '',
    '| Metric | Value |',
    '|---|---|',
    k6
      ? [
          `| Requests | ${k6.requests} (${k6.rate.toFixed(1)}/s) |`,
          `| Errors | ${(k6.failedRate * 100).toFixed(2)}% |`,
          `| Latency p50 / p95 / p99 | ${ms(k6.p50)} / ${ms(k6.p95)} / ${ms(k6.p99)} |`,
          `| Requests k6 couldn't start in time | ${k6.dropped} |`,
        ].join('\n')
      : '| k6 result | not available (see k6 output above) |',
    '',
    '**Delivery (API → outbox → RabbitMQ → worker → provider)**',
    '',
    '| Metric | Value |',
    '|---|---|',
    `| Emails | ${d.total} total: ${d.sent} sent, ${d.failed} dead-lettered, ${d.pending} still in progress |`,
    `| Needed a retry | ${d.retried} |`,
    `| Delivery throughput | ${spanS > 0 ? (d.sent / spanS).toFixed(1) : '-'} emails/s |`,
    `| End-to-end latency p50 / p95 / p99 | ${ms(d.p50)} / ${ms(d.p95)} / ${ms(d.p99)} |`,
    '',
    '_End-to-end latency = from the API accepting the email to the provider accepting it._',
  ].join('\n');

  console.log(`\n\n${report}\n`);
  if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${report}\n`);
  await pool.end();
}

main().catch(async (err) => {
  console.error('Load test failed:', err);
  await pool.end();
  process.exit(1);
});
