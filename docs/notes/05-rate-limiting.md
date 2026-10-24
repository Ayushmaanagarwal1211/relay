# Step 5: Rate limiting with Redis

## The problem

Email providers limit how fast you can send (for example, 100 emails/second on your plan). Go over it and they answer "slow down" or, worse, flag your account. We run several workers, so **the limit has to be shared by all of them**.

## What was built

- `src/shared/rate-limiter.ts`: a **token bucket** stored in Redis, updated by a Lua script
- The worker waits for a token before each send (`src/worker/run-worker.ts`, look for `throttle.acquire()`)
- Config: `PROVIDER_RATE_LIMIT_PER_SEC` (0 = off) and `PROVIDER_RATE_LIMIT_BURST`

## Files to read, in this order

1. `src/shared/rate-limiter.ts`: read the big comment first, then the Lua script line by line
2. `src/worker/run-worker.ts`: where `acquire()` is called, and why it's **before** the claim
3. `tests/integration/rate-limiter.test.ts`: each test is one property of the limiter

## The token bucket

```
        refills at 100 tokens/second
                  │
                  ▼
           ┌─────────────┐
           │ ● ● ● ● ● ● │  holds at most 100 tokens (the burst)
           └─────────────┘
                  │
       each email takes 1 token
```

- **Quiet for a while?** The bucket fills up, so the next 100 emails go out at once (a burst).
- **Busy?** Emails go out at the refill rate, 100 per second.

### Why a token bucket, and not other algorithms?

| Algorithm | How it works | Problem |
|---|---|---|
| Fixed window | Count per calendar second; reset at :00 | 100 at 0.99s + 100 at 1.01s = 200 in 20ms |
| Sliding window log | Store a timestamp per request | Memory grows with traffic |
| **Token bucket** | Tokens refill continuously | Allows a controlled burst, smooth after that. Tiny state: 2 numbers. |
| Leaky bucket | Requests leave at a fixed rate | No bursts at all; similar to token bucket otherwise |

## Why Redis?

If each worker kept its own counter in memory, 3 workers × 100/s = **300/s** to the provider. The bucket must live in one place every worker can see. Redis is fast (sub-millisecond), and every worker already connects over the network.

## Why a Lua script?

The steps are: read the bucket → add refilled tokens → take one → save. If two workers do these as separate commands:

```
Worker A: read → 1 token left
Worker B: read → 1 token left
Worker A: take it → save 0
Worker B: take it → save 0     ← two emails, one token
```

Redis runs a Lua script as **one atomic step**; nothing else runs in the middle. The test `never gives more than the capacity for free, even with 50 callers at once` proves it.

The script also uses **Redis's clock** (`TIME`), not each worker's. If one worker's clock ran 2 seconds fast, it would otherwise think more tokens had refilled.

## Reservations, a fix the load test found

**Version 1:** no token? Wait a bit and ask again. Under load, about 40 waiting emails each asked Redis every ~10ms: **thousands of Redis calls per second** just to say "not yet". API p95 rose to 419ms.

**Version 2 (now):** no token? **Take one on credit anyway.** The bucket goes below zero, and Redis answers "your token will exist in 230ms". The worker sleeps exactly 230ms and sends. **One Redis call per email**, no matter how busy.

Measuring found a real problem, and the fix was a design change, not a tweak. That's a great interview story.

## Why wait BEFORE claiming the email?

The claim gives a worker a 60-second **lease**. If we claimed first and then waited a long time for a token, the lease could expire, and another worker would take over and send the same email. Waiting first means the claim → send part is always quick.

## Backpressure

While a worker waits for a token, that message holds one of its 10 prefetch slots. Once all 10 are waiting, RabbitMQ stops giving it more messages. So:

```
API accepts 200/s → RabbitMQ queue grows → workers send 100/s
```

Nothing is dropped. The queue absorbs the difference, and emails just take longer. That's **backpressure**: the slow part pushes back, all the way to the queue.

## What if Redis is down? Fail open vs fail closed

| Choice | What happens | Risk |
|---|---|---|
| **Fail open (ours)** | Send without limiting, log a warning | Might briefly go over the provider's limit. It answers "slow down", a temporary error we already retry. |
| Fail closed | Stop sending until Redis is back | A Redis problem stops **all** email, OTPs included |

For OTPs and password resets, staying up matters more. A payment system calling a bank might choose the opposite.

To make a Redis outage fail fast instead of hanging, the connection uses `enableOfflineQueue: false` and `commandTimeout: 500ms`.

## Load test results (2 CPU cores, 2 workers)

| Test | API p95 | Delivered | Delivery rate |
|---|---|---|---|
| 200/s, no limit | 14 ms | 6,001 / 6,001 | 198/s |
| 100/s, limit 100/s | **5 ms** | 3,001 / 3,001 | 99/s |
| 200/s, limit 100/s | 253 ms | 5,956 / 5,956 | **100.2/s** |

What they show:
- **The limit holds across workers:** 2 workers together delivered 100.2/s, not 200.
- **Nothing was lost:** extra emails waited in RabbitMQ and all were delivered (end-to-end latency grew to about 17s).
- **The limiter itself is cheap:** with no backlog, API p95 was 5ms.
- **The 253ms comes from the backlog, not the limiter.** A queue growing by 100 emails/s makes RabbitMQ and the database work harder, and on this test machine everything shares 2 CPU cores with the API. On separate machines, the API wouldn't feel it.

## Two places rate limits are used (interview context)

1. **Outgoing, to protect the provider** (built): we slow ourselves down, and work waits in the queue.
2. **Incoming, to protect our API** (not built): limit each client, e.g. 10 OTPs per minute per recipient, so nobody can spam a user. When the bucket is empty, the API answers **429 Too Many Requests** with a `Retry-After` header instead of waiting. Same token bucket, different reaction.

## Try it yourself (in the codespace)

**Terminal 1**: a low limit, so it's easy to see:
```bash
PROVIDER_RATE_LIMIT_PER_SEC=2 PROVIDER_RATE_LIMIT_BURST=1 npm run dev
```
**Terminal 2**: send 10 emails at once (paste the `send` shortcut from step 3 first):
```bash
for i in $(seq 1 10); do send "Limited $i" & done; wait
```
Terminal 1 shows `sent` lines arriving about **2 per second**, not all at once.

Look at the bucket in Redis while it runs:
```bash
docker compose exec redis redis-cli HGETALL ratelimit:provider:smtp
```
`tokens` goes negative while emails are waiting: those are the reservations.

Then see fail-open: stop Redis and send again.
```bash
docker compose stop redis
for i in $(seq 1 5); do send "No redis $i" & done; wait
docker compose start redis
```
Terminal 1 shows the warning `Redis unavailable, sending without rate limiting`, and the emails go out at once.

## Practice questions

1. Why does the rate limit have to live in Redis and not in each worker?
2. Explain the token bucket. What do the rate and the capacity each control?
3. Why a Lua script? Show the race condition it prevents.
4. Why use Redis's clock instead of the worker's?
5. Fixed window vs token bucket: what goes wrong with a fixed window?
6. Your first version slowed the API under load. What was wrong, and how did reservations fix it?
7. Why does the worker wait for a token before claiming the email, not after?
8. What is backpressure? Where did the extra emails go when 200/s came in and only 100/s went out?
9. Redis goes down. What does your system do, and why did you choose that?
10. How would you rate-limit the API itself? What would the response be?
