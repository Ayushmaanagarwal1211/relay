# Relay: a reliable email delivery service

Other apps call one API to send an email. Relay makes sure every email is sent, even when servers crash, the email provider fails, or the client retries the same request.

## Architecture

```
Client app ──POST /v1/emails (Idempotency-Key header)──► API service
                    │  one DB transaction: email row + outbox row
                    ▼
             Outbox relay ──► RabbitMQ
                                ├─► transactional queue (OTPs, password resets)
                                └─► promotional queue  (newsletters)
                                          ▼
              Email worker: rate limit (Redis) → claim (skip if already sent) → send
                 failed → retry queue (wait 10s → 1m → 5m) → back to main queue
                 failed 4 times → dead-letter queue
                                          ▼
              SMTP (Mailpit on your laptop) | Mock provider (for load tests)
```

**Delivery guarantee:** at-least-once, with idempotency. See [docs/notes](docs/notes) for the reasoning behind each decision.

## Progress

- [x] Step 1: Design
- [x] Step 2: API with idempotency, plus the transactional outbox write
- [x] Step 3: Outbox relay, RabbitMQ, email worker, retries, dead-letter queue (plus CI)
- [x] Step 4: Rate limiting: token bucket in Redis, shared by all workers
- [ ] Step 5: More tests
- [x] Step 6: Mock provider and k6 load tests (failure drills: see the step 3 notes)
- [ ] Step 7: Final README and results

## Load test results

GitHub Actions runner (2 CPU cores, 7 GB RAM), mock provider with 50ms latency. Details and analysis: [docs/notes/04-load-testing.md](docs/notes/04-load-testing.md).

| Test | API p95 | Delivered | Delivery rate |
|---|---|---|---|
| 200 emails/s, 1 worker | 22 ms | 6,001 / 6,001 | 197/s |
| 200 emails/s, 10% provider failures | 6 ms | 6,001 / 6,001 (562 retried) | 176/s |
| 500 emails/s, 1 worker | 1,322 ms | 14,269 / 14,269 | 214/s |
| 500 emails/s, 2 workers | 2,694 ms | 12,957 / 12,957 | 358/s |
| 200 emails/s, 2 workers, provider limit 100/s | 253 ms | 5,956 / 5,956 | 100.2/s (limit held) |

Run it yourself: **Actions → Load test → Run workflow**, or `RATE=200 npm run load:test` locally.

## Run it

**Easiest: GitHub Codespaces** (nothing to install). On GitHub, click **Code → Codespaces → Create codespace**. Dependencies install automatically; then run:

```bash
npm run infra:up && npm run db:migrate && npm run dev
```

**On your own machine:** requires Node.js 22+ and Docker.

```bash
npm install
cp .env.example .env
npm run infra:up      # Postgres, RabbitMQ, Redis, Mailpit
npm run db:migrate
npm run dev           # API (http://localhost:3000), outbox relay and email worker
```

- Sent emails show up in Mailpit: http://localhost:8025
- RabbitMQ dashboard: http://localhost:15672 (login `relay` / `relay`)

Send an email:

```bash
curl -i -X POST localhost:3000/v1/emails \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(node -p "crypto.randomUUID()")" \
  -d '{"to":"user@example.com","subject":"Your OTP","body":"Code: 123456"}'
```

Check its status:

```bash
curl localhost:3000/v1/emails/<id>
```

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/emails` | Queue an email. Requires an `Idempotency-Key` header. |
| `GET` | `/v1/emails/:id` | Get an email's status: `queued`, `sending`, `sent` or `failed`. |
| `GET` | `/health` | Health check (also checks the database). |

`POST /v1/emails` responses:

| Status | When |
|---|---|
| `202 Accepted` | The email was queued. |
| `202 Accepted` + `Idempotent-Replayed: true` | A retry with the same key and body. Returns the original email; nothing new is queued. |
| `400 Bad Request` | Missing `Idempotency-Key`, invalid body, or malformed JSON. |
| `422 Unprocessable Entity` | The key was already used with a different body. |

## Tests

```bash
npm test                  # unit tests, no Docker needed
npm run test:integration  # needs Postgres; uses the separate relay_test database
npm run test:e2e          # needs Postgres, RabbitMQ and Mailpit; runs the whole pipeline
```

GitHub Actions runs all three on every push (`.github/workflows/ci.yml`).

## Project layout

```
src/
  api/        HTTP API: routes, validation, idempotency + outbox write
  relay/      outbox relay: moves outbox rows into RabbitMQ
  worker/     email worker: claims, sends, retries, dead-letters
  shared/     config, database pool, migrations, RabbitMQ setup
  scripts/    one-off commands (migrate)
migrations/   SQL schema, applied in filename order
tests/        unit and integration tests
docs/notes/   design decisions and interview notes for each step
```
