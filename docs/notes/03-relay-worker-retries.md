# Step 3: Outbox relay, RabbitMQ, the worker, retries and the dead-letter queue

## What was built

```
API ──► Postgres (emails + outbox)
              │
              ▼
        Outbox relay ──publish + confirm──► exchange "emails"
                                              ├─ "transactional" → emails.transactional ─┐
                                              └─ "promotional"   → emails.promotional  ──┤
                                                                                        ▼
                                                                                  Email worker
                                                                                        │
                    ┌──────────── sent ◄────────────────── claim in Postgres → send ────┤
                    │                                                                   │
              ack message            temporary failure → emails.<priority>.retry.10s / 1m / 5m
                                     (waits there, then RabbitMQ moves it back to the main queue)
                                     permanent failure or out of retries → emails.dead-letter
```

## Files to read, in this order

1. `src/relay/outbox-relay.ts`: `relayBatch`, the core of the relay. Short, and every line matters.
2. `src/shared/rabbitmq.ts`: the queues, and how retry queues work (read the comments)
3. `src/worker/retry-policy.ts`: when to retry and when to give up
4. `src/worker/process-email.ts`: **the most important file.** The claim query is the idempotency check.
5. `src/worker/run-worker.ts`: how RabbitMQ messages are acknowledged
6. `src/worker/email-provider.ts`: the provider interface, and temporary vs permanent errors
7. `tests/integration/process-email.test.ts`: each test is one failure scenario

## The relay

Every 500ms the relay runs one transaction:

1. **Lock** up to 100 unpublished outbox rows (`FOR UPDATE SKIP LOCKED`)
2. **Publish** each to RabbitMQ, then **wait for RabbitMQ to confirm** it has them
3. **Mark** them published, and commit

**Publisher confirms** are RabbitMQ saying "I have this message and saved it to disk". Without waiting for them, we could mark a row published when RabbitMQ never got the message, and lose it.

**`SKIP LOCKED`** means "skip rows another relay has already locked". You can run 3 relays for more speed or for backup, and they never publish the same row twice.

## The worker: the claim step

Before sending, the worker **claims** the email in Postgres:

```sql
UPDATE emails SET status = 'sending', attempts = attempts + 1
 WHERE id = $1
   AND (status = 'queued'
        OR (status = 'sending' AND updated_at < now() - 60 seconds))
RETURNING *
```

One query does three jobs:

| Email's status | Matches? | What it means |
|---|---|---|
| `queued` | Yes | Normal case: we own it now, send it |
| `sent` or `failed` | No | A duplicate message: skip it |
| `sending`, less than 60s ago | No | Another worker is sending it right now: check again later |
| `sending`, more than 60s ago | Yes | The worker that claimed it crashed: take over (the **lease** expired) |

Because it's one `UPDATE`, two workers can't both claim the same email. Postgres locks the row, so the second worker sees `sending` and backs off.

## Retries: why one queue per delay?

RabbitMQ has no "deliver this in 1 minute" feature built in. The standard trick:

1. Put the failed message in a queue that **nobody reads**, with a **TTL** (time to live) of, say, 10 seconds
2. When the TTL runs out, RabbitMQ **dead-letters** the message to the exchange we configured, with the routing key we configured
3. It lands back in the main queue, and a worker tries again

**Why not one retry queue with a different TTL on each message?** RabbitMQ only expires messages at the **front** of a queue. A 5-minute message at the front would hold back a 10-second message behind it for 5 minutes. With one queue per delay, everything in a queue has the same TTL, so they expire in order.

**Why do the delays grow (10s → 1m → 5m)?** It's called **exponential backoff**. If the email provider is down, retrying every second just adds load and makes it worse. Waiting longer each time gives it room to recover.

## Temporary vs permanent errors

| Error | Example | What we do |
|---|---|---|
| Network error | Can't connect, timeout | Retry: the server may come back |
| SMTP 4xx | `421 try again later`, rate limited | Retry |
| SMTP 5xx | `550 no such user` | **Dead-letter at once**: retrying will never work |

## When is a message acknowledged (acked)?

**Only after we're completely done:** the email is sent and recorded, or a retry or dead-letter copy has been published **and confirmed**. If the worker crashes before acking, RabbitMQ gives the message to another worker. That's why nothing is ever lost.

**Prefetch = 10** means each worker holds at most 10 unacknowledged messages at a time. It's the worker's concurrency limit, so a slow provider can't make one worker pull in thousands of messages.

## Every failure, and what happens

| What fails | What happens | Email lost? | Sent twice? |
|---|---|---|---|
| API crashes mid-request | Transaction rolls back, client gets an error and retries with the same key | No | No |
| Relay crashes after publishing, before marking | Row published again → duplicate message → worker skips it (`already-sent`) | No | No |
| RabbitMQ down | Relay can't publish; rows wait in the outbox | No | No |
| Worker crashes before sending | Message wasn't acked → redelivered → lease expires → sent | No | No |
| **Worker crashes after sending, before recording "sent"** | Lease expires → another worker sends it again | No | **Yes, the one unavoidable case** |
| Email provider down | Retries 10s → 1m → 5m, then dead-letter queue | No (kept in the DLQ) | No |
| Postgres down while working | Message put back on the queue, tried again after 1s | No | No |

That one "Yes" is why we say **at-least-once**. To make it even rarer, each email has the same `Message-ID` on every attempt, so many mail servers can spot the duplicate.

## Practice questions

1. Walk me through what happens from `POST /v1/emails` to the email arriving in the inbox.
2. What are publisher confirms, and what would go wrong without them?
3. How do you run three relays without publishing the same row three times?
4. Two workers get the same message at the same moment. Why is the email sent only once?
5. A worker crashes in the middle of sending. What happens to that email?
6. How do you do delayed retries in RabbitMQ? Why one queue per delay?
7. Why exponential backoff and not "retry every second"?
8. Which errors do you retry, and which don't you? Why?
9. When do you ack a message, and why not as soon as you receive it?
10. What's prefetch, and what happens if you set it to 1000?
11. What's in the dead-letter queue, and what would you do with it in production?
12. Where can an email still be sent twice? Can you eliminate that case?

## Try it yourself (in the codespace)

**Terminal 1**: start everything.
```bash
git pull && npm install
npm run infra:up && npm run db:migrate
npm run dev
```
You'll see three coloured streams: `[api]`, `[relay]` and `[worker]`.

**Terminal 2**: run these.

### 1. Send an email and watch it arrive
```bash
curl -s -X POST localhost:3000/v1/emails -H "Content-Type: application/json" -H "Idempotency-Key: $(node -p "crypto.randomUUID()")" -d '{"to":"user@example.com","subject":"Hello","body":"My first email"}'
```
In terminal 1: `[relay] published 1 message(s)`, then `[worker] ... sent via smtp (attempt 1)`.
Open the inbox: **Ports** tab (next to Terminal) → port **8025** → click the globe icon. Your email is there.

### 2. Make the email server go down, and watch the retries
```bash
docker compose stop mailpit
curl -s -X POST localhost:3000/v1/emails -H "Content-Type: application/json" -H "Idempotency-Key: $(node -p "crypto.randomUUID()")" -d '{"to":"user@example.com","subject":"Retry test","body":"Will this arrive?"}'
```
Terminal 1 shows `attempt 1 failed ... retrying in 10000ms`, then about 10s later `attempt 2 failed ... retrying in 60000ms`.
Now bring the server back **within a minute**:
```bash
docker compose start mailpit
```
The next attempt succeeds: `sent via smtp (attempt 3)`. Check the inbox.

### 3. Watch an email end up in the dead-letter queue
Waiting 10s + 1m + 5m is slow, so use short delays. In `.env`, add (or change) the line `RETRY_DELAYS_MS=3000,3000`. Then restart terminal 1 (Ctrl+C, `npm run dev`).
```bash
docker compose stop mailpit
curl -s -X POST localhost:3000/v1/emails -H "Content-Type: application/json" -H "Idempotency-Key: $(node -p "crypto.randomUUID()")" -d '{"to":"user@example.com","subject":"Doomed","body":"This will fail"}'
```
After 3 attempts, terminal 1 shows `gave up after 3 attempt(s) (max-attempts)`.
Open the RabbitMQ dashboard: **Ports** tab → port **15672** → globe icon, and log in as `relay` / `relay`. Go to **Queues** → `emails.dead-letter` → **Get messages**. Look at the headers: `x-error`, `x-reason`, `x-attempts`.
Afterwards: `docker compose start mailpit`, and set `RETRY_DELAYS_MS` back to `10000,60000,300000`.

### 4. See the outbox protect you while RabbitMQ is down
```bash
docker compose stop rabbitmq
```
The relay and worker exit (that's on purpose). Send 3 emails with the curl from exercise 1. The API still accepts them (202). Check the outbox:
```bash
docker compose exec postgres psql -U relay -c "SELECT count(*) AS waiting FROM outbox WHERE published_at IS NULL;"
```
Now bring RabbitMQ back, then restart terminal 1:
```bash
docker compose start rabbitmq
```
The relay publishes all the waiting rows, and all 3 emails arrive. **Nothing was lost while the queue was down.**

### 5. Run the tests
```bash
npm test && npm run test:integration && npm run test:e2e
```
