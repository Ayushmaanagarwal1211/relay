# Step 6: Stress testing (steady, spike, chaos, soak)

The load tests in [04-load-testing.md](04-load-testing.md) answer "how fast is it?". These answer the harder questions:

| Test | Question it answers |
|---|---|
| **steady** | What's the highest rate it can hold for minutes, without falling behind? |
| **spike** | What happens when traffic suddenly jumps 10×? |
| **chaos** | If parts of the system crash in the middle, is any email lost? Sent twice? |
| **soak** | Does anything slowly get worse over a long run (memory leaks)? |

## What was built

- `src/scripts/stress-test.ts`: one command per test. It starts everything itself, runs k6, crashes things on purpose (chaos), waits until every email is delivered, then writes a report.
- `src/api/server.ts`: the API can now run as several processes (`API_PROCESSES`), using Node's **cluster** module.
- `src/worker/mock-provider.ts`: the fake provider can record every email it gets in Redis, so the test can **count lost and duplicate emails**.
- `load/send-emails.js`: a spike scenario, plus client retries that reuse the same `Idempotency-Key`.
- `.github/workflows/stress-test.yml`: the same tests with one click on GitHub (small machine).

## Step by step: run it in an 8-core codespace

GitHub's 2-core machines are too small to show what the system can really do, so use a bigger codespace. Your free plan includes **120 core-hours a month**. An 8-core codespace uses 8 per hour, so all the tests below (about 1.5 hours) use about 12.

### 1. Create an 8-core codespace

1. Open the repo on GitHub → green **Code** button → **Codespaces** tab
2. Click **...** (next to the **+**) → **New with options...**
3. **Machine type: 8-core** → **Create codespace**
4. Wait for setup to finish (a few minutes, like the first time)
5. In the terminal, check you got 8 cores:
   ```bash
   nproc
   ```

Already have a codespace? You can change it instead: go to github.com/codespaces → **...** next to it → **Change machine type** → 8-core. Then open it and run `git pull && npm install`.

### 2. A quick first run (3 minutes)

```bash
RATE=300 DURATION=1m npm run stress -- steady
```

The first time, Docker downloads images, so it takes a bit longer. You'll see one line every 2 seconds:

```
[0:24] accepted   300/s | delivered   301/s | waiting: outbox 0, RabbitMQ 12
```

- **accepted**: emails the API saved per second
- **delivered**: emails the (fake) provider received per second
- **waiting**: emails not sent yet. Small and steady = keeping up. Growing = falling behind.

At the end it prints the report and saves it in `load-results/`. Check the first bold line says **0 lost**.

### 3. Find the maximum rate (about 10 minutes)

Run 1-minute tests, going up:

```bash
RATE=500 DURATION=1m npm run stress -- steady
RATE=1000 DURATION=1m npm run stress -- steady
RATE=1500 DURATION=1m npm run stress -- steady
RATE=2000 DURATION=1m npm run stress -- steady
```

A rate is **healthy** if, in its report:
- **Requests k6 couldn't start in time** is 0, and nothing was rejected
- **Delivery throughput** (average) is close to the rate you asked for
- **Waiting** in the live lines stays small instead of growing
- **API p99** is still small (under about 100 ms)

The highest healthy rate is your **maximum**. Stop going up once one isn't healthy. That's the point where something became the bottleneck, which is useful too (see "Reading the results" below).

### 4. The headline run (about 12 minutes)

Your maximum (or a bit under it), for 10 minutes:

```bash
RATE=1000 DURATION=10m npm run stress -- steady
```

At 1,000/s, that's 600,000 emails. This is the number for your resume.

### 5. The spike test (about 3 minutes)

Normal traffic, then a burst of more than the system can handle, then normal again:

```bash
BASE_RATE=200 PEAK_RATE=3000 npm run stress -- spike
```

What to look at: the API stays fast during the burst (it only writes to Postgres), the extra emails wait in RabbitMQ (**Most emails waiting at once**), and **All delivered within** tells you how fast the backlog cleared.

### 6. The chaos test (about 5 minutes)

Steady load at about 70% of your maximum, while the script crashes things:

```bash
RATE=700 DURATION=4m npm run stress -- chaos
```

| When | What it does | What should happen |
|---|---|---|
| 20% in | `kill -9` a worker | Its unacknowledged messages go back to RabbitMQ; another worker takes them. The supervisor restarts it. |
| 40% in | `kill -9` the outbox relay | New emails wait in the outbox table; the restarted relay publishes them. |
| 60% in | Kill RabbitMQ (`docker kill`), start it 10s later | The API **keeps accepting emails** (it doesn't talk to RabbitMQ). They wait in the outbox. Workers and relay crash, restart and reconnect. |
| 80% in | `kill -9` one API process | Its open requests fail; k6 retries them with the **same Idempotency-Key**, so nothing is saved twice. |

Look at: **Lost** (must be 0), **Duplicate sends** (a few is expected, see below), and the **Chaos events** table.

### 7. The soak test (optional, 32 minutes)

```bash
RATE=300 DURATION=30m npm run stress -- soak
```

Look at the **Memory** table: Start, Max and End should be close. Memory that keeps growing would be a leak.

### 8. Save the results and stop the codespace

```bash
git add load-results
git commit -m "Stress test results (8-core codespace)"
git push
```

Then **stop the codespace** (github.com/codespaces → **...** → **Stop codespace**). A running 8-core codespace uses your free hours even when you're not typing.

Send me the reports (or just push them) and I'll put the numbers in the README and on your resume.

### While a test runs (optional)

Open the **Ports** tab → port **15672** (RabbitMQ, login `relay` / `relay`) → **Queues**, and pick the **load-test** virtual host at the top right. During the spike or chaos test you can watch the queue fill up and drain.

## How it works

### It runs in its own database and RabbitMQ "vhost"

The test uses the database `relay_load` and the RabbitMQ virtual host `load-test` (a separate set of queues), both wiped at the start of each run. Your dev data isn't touched, and a dev worker can't steal the test's messages.

### It restarts crashed processes, like Kubernetes

The script starts every process itself and, if one dies, starts it again a second later. In production, Kubernetes or systemd does this. Without it, the chaos test would only prove that a dead process stays dead.

### How "lost" and "duplicate" are counted

The fake provider adds every email id it receives to a Redis set (`SADD`):
- `SADD` returns 0 if the id was already there: the provider got that email twice, so that's a **duplicate send**
- the size of the set (`SCARD`) = **unique emails delivered**

Then: **lost** = emails the API accepted (202) − emails delivered − emails dead-lettered. This doesn't trust our own database's `status` column; it checks what the "provider" actually got.

### Why duplicates can happen, and why that's correct

`kill -9` on a worker can land in this gap:

```
worker: provider.send() succeeds ──► (killed here) ──► UPDATE emails SET status = 'sent'
```

The provider has the email, but our database never recorded it. After the lease (`SENDING_LEASE_MS`, 10s in these tests) runs out, another worker sends it again. This is the at-least-once guarantee from step 1: **we'd rather send twice than never**. No design can close this gap with a provider that doesn't take an idempotency key. The same `Message-ID` on every attempt lets mail servers spot the copy.

So for the chaos test, a handful of duplicates is the expected, honest result. Explaining that number is better than claiming exactly-once.

### Why several API processes?

Node runs your JavaScript on one CPU core per process. One API process on an 8-core machine can only use one core. The `cluster` module starts several processes that share the same port. This works only because the API is **stateless**: everything lives in Postgres, so any process can handle any request.

### Settings the stress tests change

Compared to `npm run dev`, the script uses (all can be overridden with an environment variable):

| Setting | Stress test | Why |
|---|---|---|
| `WORKER_PREFETCH` | 100 (was 10) | Little's Law: 100 at once ÷ 0.05 s = up to 2,000 emails/s per queue per worker |
| `API_PROCESSES` / `WORKERS` | cores ÷ 4 each | Use the machine's cores |
| `DB_POOL_SIZE` | 20 | More processes doing more at once need more connections |
| `RELAY_BATCH_SIZE` / poll | 500 / 100 ms | Fewer round trips per email; lower end-to-end latency |
| `SENDING_LEASE_MS` / retry delays | 10 s / 1s, 2s, 4s | Crashed work is picked up in seconds, so the test doesn't take hours |
| Postgres `max_connections` | 300 | The default 100 is too few for all the processes |

What we did **not** change: Postgres still writes every commit to disk (`synchronous_commit` stays on), and RabbitMQ messages stay persistent. Turning those off would make the numbers faster and the "0 lost" claim fake.

## Reading the results

- **Delivery can't keep up, but the API is fine**: the workers are the bottleneck. Add workers, or raise the prefetch.
- **The API gets slow, and k6 can't start requests in time**: the API or Postgres is the bottleneck. More API processes help only if CPU is free. Check with `top` in another terminal while the test runs.
- **Everything slows together**: the machine is out of CPU. k6, Postgres, RabbitMQ and the app all share the same 8 cores. That's a limit of the test setup, not of the design. In production they'd be on separate machines.

## Results

### First check: GitHub's 2-core runner (small, to prove the tests work)

| Test | Accepted | Delivered | Lost | Duplicates | API p99 |
|---|---|---|---|---|---|
| steady, 300/s for 1m | 17,948 | 17,948 | 0 | 0 | 207 ms |
| spike, 100 → 800 → 100/s | 39,187 | 39,187 | 0 | 0 | 2,639 ms |
| chaos, 200/s for 2m | 23,869 | 23,869 | 0 | 0 | 245 ms |

Chaos events in that run:

| Time | What happened | Effect |
|---|---|---|
| 0:24 | kill -9 worker-1 | delivery dropped to 102/s, back to normal 3s after |
| 0:48 | kill -9 the outbox relay | delivery dropped to 159/s, back to normal 4s after |
| 1:12 | killed RabbitMQ, restarted 10s later | delivery dropped to 0/s, back to normal 19s after. The API kept accepting 200/s the whole time; 3,390 emails waited in the outbox. |
| 1:36 | kill -9 the API (1 process on 2 cores) | k6 retried 280 requests with the same Idempotency-Key; 0 rejected, 0 saved twice |

What the 2-core runner shows: everything (k6, Postgres, RabbitMQ, the app) shares 2 cores, so at 800/s the API slowed to seconds. The spike numbers need the bigger machine.

### 8-core codespace

_Your runs go here._

## Practice questions

1. What's the difference between a load test, a spike test, a chaos test and a soak test? What does each one catch?
2. How did you find your maximum rate? What was the bottleneck there, and how did you know?
3. During the spike, the API stayed fast even though the workers couldn't keep up. Why?
4. You killed RabbitMQ in the middle of the test and the API kept returning 202. How?
5. A worker was killed with `kill -9`. Walk through what happens to the emails it was sending.
6. Your chaos test had a few duplicate sends. Why is that the correct behavior, and how would you reduce the impact?
7. How did you count lost emails? Why not just trust the `status` column?
8. Why does Node need the cluster module to use 8 cores? What makes the API safe to run as several processes?
9. You raised the prefetch from 10 to 100. Use Little's Law to explain what that changes.
10. Why didn't you turn off `synchronous_commit` to get a faster number?
11. Your test ran k6, Postgres, RabbitMQ and the app on one machine. How does that affect the numbers?
12. What would you change to reach 10,000 emails/s?
