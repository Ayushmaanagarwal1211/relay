# Step 1: Design decisions

Each decision below is something an interviewer can ask about. Learn the **why**, not just the what.

## 1. Delivery guarantee: at-least-once, with idempotency

**Decision:** Every email is sent at least once. Duplicates are possible but rare, and we work to prevent them.

**Why not exactly-once?** Sending an email and recording "sent" in our database happen in two different systems (the email server and Postgres). There's always a moment between the two. If the worker crashes there, the email went out but our database still says "sending". On retry, we send it again.

No code can close that gap, because we can't put the email server and Postgres in one transaction. So "exactly-once delivery" to an outside system is impossible. What we can do:
- **Never lose an email** (at-least-once): retries, the outbox, and only removing a job from the queue after it succeeds.
- **Make duplicates rare** (idempotency): the API ignores repeated requests, and the worker skips emails already marked `sent`.

**Why not at-most-once?** It means "send once, never retry". It's simpler, but a single failure loses the email. For an OTP or password reset, a lost email is worse than a rare duplicate.

## 2. Transactional outbox

**Problem (the "dual write"):** The API must (1) save the email in Postgres and (2) put a job in the queue. These are two systems. If the server crashes between them, the database says "queued" but no job exists, and the email is silently never sent.

**Decision:** The API never talks to the queue. In **one database transaction** it writes the email row **and** an outbox row. A separate process, the **outbox relay**, reads unpublished outbox rows, publishes them to RabbitMQ, and marks them published.

**What if the relay crashes after publishing but before marking the row?** It publishes the same row again: a duplicate job. That's fine, because the worker is idempotent (it skips emails already `sent`). The outbox prevents **lost** messages; idempotency makes **duplicate** messages harmless. They work as a pair.

**Trade-off:** Emails reach the queue slightly later (up to one polling interval, about 1 second), and there's one more process to run. Worth it for never losing an email.

## 3. RabbitMQ, not Kafka

| | RabbitMQ | Kafka |
|---|---|---|
| Model | Job queue: a message is handled once, then deleted | Log: messages stay; many readers can re-read them |
| Delayed retries | Built in (message TTL + dead-letter exchange) | Not built in; you build retry topics yourself |
| Priority | Separate queues or priority queues | Awkward |
| Best for | "Do this task once, reliably" | Event streams, replay, very high throughput |

**Decision:** Sending an email is a task, so RabbitMQ fits. Choose Kafka when several services need to react to the same events, or you need to replay history.

## 4. Priority: separate queues

OTPs and password resets go to the `transactional` queue. Newsletters go to `promotional`. Each queue has its own workers, so a 100,000-email newsletter can't delay someone's OTP.

## 5. Email only, behind a provider interface

The worker calls a `send()` interface, not a specific provider. We implement:
- **SMTP**, pointed at Mailpit on your laptop (a fake email server with a web inbox)
- **Mock provider** for load tests, with configurable latency and failure rate

Adding WhatsApp or SMS later means writing one new provider class. That's the answer to "How would you add a new channel?"

## 6. Three services in one repo

- **API**: accepts requests and writes to Postgres
- **Outbox relay**: moves outbox rows into RabbitMQ
- **Email worker**: sends the emails

They share code (config, database access) but run as separate processes, so each can scale and fail on its own. For example, you could run 10 workers but only 2 API instances.

## 7. Stack

TypeScript, Node.js (Express), PostgreSQL, Redis, RabbitMQ, Docker Compose, Jest, k6, GitHub Actions.

## Practice questions

1. Why is exactly-once delivery impossible here? What do you do instead?
2. What is the dual-write problem? How does the outbox solve it?
3. The relay publishes a message, then crashes before marking it published. What happens next, and why is it safe?
4. Why RabbitMQ over Kafka for this? When would you pick Kafka?
5. How do you stop a newsletter blast from delaying OTPs?
