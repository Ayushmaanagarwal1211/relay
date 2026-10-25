// k6 load test: sends POST /v1/emails.
// Don't run this file directly; use `npm run load:test` or `npm run stress -- <scenario>`
// (they clean up, run k6 in Docker, wait for the workers to finish, and print a report).
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const SCENARIO = __ENV.SCENARIO || 'steady'; // steady | spike
const RATE = Number(__ENV.RATE || 100); // requests per second (steady)
const DURATION = __ENV.DURATION || '30s';
const BASE_RATE = Number(__ENV.BASE_RATE || 200); // spike: normal traffic
const PEAK_RATE = Number(__ENV.PEAK_RATE || 2000); // spike: the burst
const API_URL = __ENV.API_URL || 'http://localhost:3000';
const PROMO_SHARE = Number(__ENV.PROMO_SHARE || 0.2); // 20% newsletters, 80% OTPs

const accepted = new Counter('emails_accepted');
const rejected = new Counter('emails_rejected');
const clientRetries = new Counter('client_retries');

const maxRate = SCENARIO === 'spike' ? PEAK_RATE : RATE;
// VUs = virtual users = how many requests k6 can have waiting at once
const vus = {
  preAllocatedVUs: Math.min(Math.max(50, Math.ceil(maxRate / 5)), 1000),
  maxVUs: Math.min(Math.max(100, maxRate * 2), 2000),
};

const scenarios = {
  // "constant-arrival-rate": start RATE requests every second, no matter how slow
  // the API gets. That's how real traffic behaves: users don't wait for your server.
  steady: {
    executor: 'constant-arrival-rate',
    rate: RATE,
    timeUnit: '1s',
    duration: DURATION,
    ...vus,
  },
  // Normal traffic, then a sudden burst (e.g. a flash sale sends everyone an OTP), then normal again
  spike: {
    executor: 'ramping-arrival-rate',
    startRate: BASE_RATE,
    timeUnit: '1s',
    stages: [
      { target: BASE_RATE, duration: '30s' },
      { target: PEAK_RATE, duration: '10s' },
      { target: PEAK_RATE, duration: '30s' },
      { target: BASE_RATE, duration: '10s' },
      { target: BASE_RATE, duration: '60s' },
    ],
    ...vus,
  },
};

export const options = {
  discardResponseBodies: true,
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  scenarios: { send_emails: scenarios[SCENARIO] },
  thresholds: {
    http_req_failed: ['rate<0.01'], // under 1% errors
    http_req_duration: ['p(95)<200'], // 95% of requests under 200ms
  },
};

export default function () {
  const priority = Math.random() < PROMO_SHARE ? 'promotional' : 'transactional';
  const body = JSON.stringify({
    to: `user${__VU}@example.com`,
    subject: `load-test ${__VU}-${__ITER}`,
    body: 'Load test email',
    priority,
  });
  // One key per email. A retry reuses the SAME key, like a real client should:
  // if the first request actually succeeded, the API replays it instead of queueing a copy.
  const params = { headers: { 'Content-Type': 'application/json', 'Idempotency-Key': crypto.randomUUID() } };

  let res;
  for (let attempt = 1; attempt <= 4; attempt++) {
    res = http.post(`${API_URL}/v1/emails`, body, params);
    // Retry only network errors (status 0) and server errors (5xx), waiting 0.5s, 1s, 2s
    if (res.status !== 0 && res.status < 500) break;
    if (attempt < 4) {
      clientRetries.add(1);
      sleep(0.5 * 2 ** (attempt - 1));
    }
  }
  if (res.status === 202) accepted.add(1);
  else rejected.add(1);
  check(res, { 'status is 202': (r) => r.status === 202 });
}

// Print a short summary, plus one machine-readable line that the test scripts read.
export function handleSummary(data) {
  const m = data.metrics;
  const d = m.http_req_duration.values;
  const count = (name) => (m[name] ? m[name].values.count : 0);
  const result = {
    requests: m.http_reqs.values.count,
    rate: m.http_reqs.values.rate,
    failedRate: m.http_req_failed.values.rate,
    p50: d.med,
    p95: d['p(95)'],
    p99: d['p(99)'],
    max: d.max,
    dropped: count('dropped_iterations'),
    accepted: count('emails_accepted'),
    rejected: count('emails_rejected'),
    clientRetries: count('client_retries'),
  };
  return { stdout: `\nK6_RESULT ${JSON.stringify(result)}\n` };
}
