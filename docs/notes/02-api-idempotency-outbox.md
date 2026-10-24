# Step 2: API, idempotency, and the outbox write

## What was built

- `POST /v1/emails`: validates the request, then saves the email **and** an outbox row in one transaction
- `GET /v1/emails/:id`: returns an email's status
- Database tables: `emails` and `outbox` (see `migrations/001_create_emails_and_outbox.sql`)
- Tests: 11 unit tests and 9 integration tests

## Files to read, in this order

1. `migrations/001_create_emails_and_outbox.sql`: the two tables. Note the `UNIQUE` on `idempotency_key`.
2. `src/api/schemas.ts`: what a valid request looks like
3. `src/api/emails.routes.ts`: the HTTP layer and status codes
4. `src/api/emails.service.ts`: **the core of this step.** Read `createEmail` slowly.
5. `tests/integration/emails.api.test.ts`: proof that it works, including 20 requests at once

## How idempotency works

The client creates a unique key (usually a UUID) for each email it wants sent, and sends that same key on every retry.

```
Request arrives with key K
  │
  ▼
INSERT email with key K ... ON CONFLICT (idempotency_key) DO NOTHING
  │
  ├── row inserted?  → new request → also insert outbox row → COMMIT → 202
  │
  └── nothing inserted? → key K already exists → load that email
          ├── same body hash?      → client retry → 202 + Idempotent-Replayed: true
          └── different body hash? → client bug   → 422
```

### What happens when 20 identical requests arrive at the same moment?

All 20 try to `INSERT` with the same key. The `UNIQUE` constraint means only one insert can win. The other 19 **wait** at that line until the first transaction commits, then see the conflict and return the original email. Result: exactly 1 email and 1 outbox row. The test `creates exactly one email when 20 identical requests arrive at the same time` proves this.

### Why check the database instead of "SELECT first, INSERT if missing"?

"Check, then insert" has a race. Two requests both SELECT, both see nothing, and both INSERT, giving a duplicate. Letting the database's `UNIQUE` constraint decide removes the race, because the check and the insert happen as one atomic step.

### Why store the key in Postgres and not Redis?

The key has to be saved **in the same transaction** as the email. If it lived in Redis, we'd have a dual-write problem again: the Redis write could succeed while the Postgres transaction fails, or the other way round.

### Why hash the request body?

To catch a client reusing a key for a **different** email. Without the hash, we'd return the old email and the new one would be silently dropped. A 422 makes the bug visible.

## Why the outbox row is in the same transaction

`withTransaction` in `src/shared/db.ts` wraps both inserts in `BEGIN ... COMMIT`. If anything fails, `ROLLBACK` undoes both. So there is never an email without an outbox row, or an outbox row without an email.

The outbox payload contains **only the email's id**, not the whole email. The worker will read the rest from Postgres. That keeps queue messages small, and Postgres stays the single source of truth for the email's status.

## Status codes

- **202 Accepted**, not 201 Created: the email is queued, not sent. 202 tells the client "accepted for processing, check back later".
- **422** for a reused key with a different body: the request is valid JSON but conflicts with an earlier one.

## Known trade-offs (good to mention in interviews)

- **Keys never expire.** Real APIs (Stripe, for example) forget keys after about 24 hours so the table doesn't grow forever. A cleanup job would fix that.
- **No authentication.** In production, each client would have an API key, and idempotency keys would be unique **per client**, not globally.
- **The response is rebuilt, not stored.** A retry gets the email's **current** status (it might already be `sent`), not a byte-for-byte copy of the first response. That's arguably more useful, but it's different from what Stripe does.

## Practice questions

1. A client's request times out, so it retries. Walk me through what happens on the server.
2. Two identical requests arrive at the same millisecond. Why don't you get two emails?
3. Why not SELECT first, then INSERT if the key is missing?
4. Why not keep idempotency keys in Redis? It's faster.
5. Why does the API return 202 and not 201?
6. What goes in the outbox payload, and why so little?
7. What would you change to support multiple client apps?

## Try it yourself

1. Send the same `curl` twice with the same key. The second response has `Idempotent-Replayed: true`.
2. Change the body but keep the key. You get a 422.
3. Open the database and look at both tables:
   `docker compose exec postgres psql -U relay -c "SELECT id, status FROM emails; SELECT * FROM outbox;"`
4. Run 20 requests with the same key at once, then count the rows. You should see exactly 1 in each table:
   ```bash
   K=$(node -p "crypto.randomUUID()"); for i in $(seq 1 20); do curl -s -o /dev/null -X POST localhost:3000/v1/emails -H "Content-Type: application/json" -H "Idempotency-Key: $K" -d '{"to":"a@b.com","subject":"Hi","body":"Hello"}' & done; wait
   docker compose exec postgres psql -U relay -c "SELECT count(*) FROM emails WHERE idempotency_key = '$K';"
   ```
