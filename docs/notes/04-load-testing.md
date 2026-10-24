# Load testing

## The easiest way: one click on GitHub

1. Open the repo on GitHub → **Actions** tab
2. On the left, click **Load test**
3. Click **Run workflow** (right side), choose the settings, click the green **Run workflow**
4. Wait about 3 minutes, click the run, and the results table is on its **Summary** page

Settings you can change:

| Setting | Meaning | Default |
|---|---|---|
| rate | Emails per second sent to the API | 200 |
| duration | How long to send for | 30s |
| workers | How many worker processes | 1 |
| mock_latency_ms | How long the fake provider takes per email | 50 |
| mock_failure_rate | Fraction of sends that fail (0.1 = 10%) | 0 |
| provider_rate_limit | Max emails/s to the provider, across all workers (0 = off) | 1000 |

Nothing to install, and it doesn't use your Codespaces hours. It uses GitHub Actions minutes instead; a free account includes 2,000 a month for private repos, and one run takes about 3.

## In the codespace (to watch it live)

**Terminal 1**: start the app with the fake provider and quiet logs:
```bash
EMAIL_PROVIDER=mock LOG_LEVEL=warn npm run dev
```
**Terminal 2**: run the test:
```bash
RATE=200 DURATION=30s npm run load:test
```
While it runs, open the RabbitMQ dashboard (port 15672) → **Queues** to watch messages flow through.

## What's measured

| Number | What it tells you |
|---|---|
| API latency p50 / p95 / p99 | How fast `POST /v1/emails` answers. p95 = 95% of requests were faster than this. |
| Errors | Requests that didn't get a 202 |
| Requests k6 couldn't start in time | k6 wanted to send more but the API was too slow to keep up. Above 0 means overloaded. |
| Delivery throughput | Emails actually sent per second by the workers |
| End-to-end latency | Time from the API accepting an email to the provider accepting it |
| Needed a retry | Emails whose first attempt failed |

**Why a mock provider?** We're measuring **our** system (API, outbox, relay, RabbitMQ, workers), not Mailpit's speed or a real provider's rate limits. The mock waits 50ms per email, like a real provider's network round trip.

**Why p95 and not the average?** Averages hide the slow requests. If 1 in 20 users waits 2 seconds, the average can still look fine. p95 shows it.

## Results (GitHub Actions runner: 2 CPU cores, 7 GB RAM)

Everything runs on that one machine: k6, Postgres, RabbitMQ, the API, the relay and the workers.

| Test | API p95 | Delivered | Delivery rate | End-to-end p50 | Notes |
|---|---|---|---|---|---|
| 200/s, 1 worker | **22 ms** | 6,001 / 6,001 | 197/s | 536 ms | Keeps up comfortably, 0 errors |
| 200/s, 1 worker, **10% provider failures** | 6 ms | 6,001 / 6,001 | 176/s | 538 ms | 562 emails retried, **0 lost** |
| 500/s, 1 worker | 1,322 ms | 14,269 / 14,269 | 214/s | 16.7 s | Worker is the bottleneck: queue builds up |
| 500/s, **2 workers** | 2,694 ms | 12,957 / 12,957 | 358/s | 1.1 s | Throughput +68%; now the 2 CPU cores are the limit |

## What the results mean

### 1. Why is one worker capped at about 200 emails/s?

**Little's Law**: concurrency = throughput × latency.

One worker handles 10 emails at once (prefetch 10) on the transactional queue, and each takes 50ms:

```
throughput = concurrency / latency = 10 / 0.05s = 200 emails/s
```

That matches the measured 214/s (a little more, because promotional emails have their own 10 slots). **This is a great interview answer**: you predicted the limit with a formula, then confirmed it by measurement.

To go faster: add workers (scale out), or raise prefetch (more emails at once per worker).

### 2. Why is end-to-end latency about 500ms even when nothing is busy?

The outbox relay checks for new rows every **500ms**. An email waits for the next check, so it sits there up to 500ms. That's the cost of the outbox pattern.

Ways to lower it, and their trade-offs:
- Poll more often (e.g. 50ms): simple, but more database queries when idle
- Postgres `LISTEN/NOTIFY`: the API wakes the relay instantly; more moving parts
- Change data capture (e.g. Debezium reads the Postgres log): no polling at all, but a whole extra system to run

### 3. At 500/s with 1 worker, nothing was lost

The API accepted emails faster than the worker could send them. The extra emails simply **waited in RabbitMQ** (that's why end-to-end latency grew to 16s), and all 14,269 were delivered. That's the point of a queue: it absorbs bursts instead of dropping work.

### 4. Adding a worker raised throughput 214 → 358/s, but not 2×

The second worker helped a lot, and end-to-end latency dropped from 16.7s to 1.1s. It didn't double, because all the processes share 2 CPU cores, and the API got slower as the CPU filled up. In production, workers run on separate machines, so they'd scale much closer to linearly.

### 5. 10% provider failures: zero emails lost

562 emails (about 9.4%) failed their first attempt and went through the retry queues. All 6,001 were delivered.

Rate-limit results are in [05-rate-limiting.md](05-rate-limiting.md).

## Numbers you can put on your resume

Use only numbers you have measured. From these runs:
- Sustained **200 emails/s with 0 errors and 22ms p95 API latency** on a 2-vCPU machine
- **0 emails lost** with 10% simulated provider failures (562 recovered through retries)
- **Scaled from 214 to 358 emails/s** by adding a second worker

## Practice questions

1. What's the difference between throughput and latency?
2. Why look at p95/p99 instead of the average?
3. Use Little's Law to explain why one worker tops out at about 200 emails/s. How would you double it?
4. Why is end-to-end latency about 500ms when the system is idle? How would you reduce it, and what does each option cost?
5. At 500/s with one worker, the API kept accepting emails. Where did the extra emails go?
6. Two workers gave 1.7× the throughput, not 2×. Why?
7. Why test with a mock provider instead of a real one?
8. What would you change to handle 5,000 emails/s?
