// k6 load test: sends POST /v1/emails at a fixed rate.
// Don't run this file directly; use `npm run load:test` (it cleans up, runs k6 in Docker,
// waits for the workers to finish, and prints a report).
import http from 'k6/http';
import { check } from 'k6';

const RATE = Number(__ENV.RATE || 100); // requests per second
const DURATION = __ENV.DURATION || '30s';
const API_URL = __ENV.API_URL || 'http://localhost:3000';
const PROMO_SHARE = Number(__ENV.PROMO_SHARE || 0.2); // 20% newsletters, 80% OTPs

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    send_emails: {
      // "constant-arrival-rate": start RATE requests every second, no matter how slow
      // the API gets. That's how real traffic behaves: users don't wait for your server.
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Math.min(RATE, 50),
      maxVUs: Math.max(RATE * 2, 100),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'], // under 1% errors
    http_req_duration: ['p(95)<200'], // 95% of requests under 200ms
  },
};

export default function () {
  const priority = Math.random() < PROMO_SHARE ? 'promotional' : 'transactional';
  const res = http.post(
    `${API_URL}/v1/emails`,
    JSON.stringify({
      to: `user${__VU}@example.com`,
      subject: `load-test ${__VU}-${__ITER}`,
      body: 'Load test email',
      priority,
    }),
    {
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': crypto.randomUUID(),
      },
    },
  );
  check(res, { 'status is 202': (r) => r.status === 202 });
}

// Print a short summary, plus one machine-readable line that `npm run load:test` reads.
export function handleSummary(data) {
  const m = data.metrics;
  const d = m.http_req_duration.values;
  const result = {
    requests: m.http_reqs.values.count,
    rate: m.http_reqs.values.rate,
    failedRate: m.http_req_failed.values.rate,
    p50: d.med,
    p95: d['p(95)'],
    p99: d['p(99)'],
    max: d.max,
    dropped: m.dropped_iterations ? m.dropped_iterations.values.count : 0,
  };
  return { stdout: `\nK6_RESULT ${JSON.stringify(result)}\n` };
}
