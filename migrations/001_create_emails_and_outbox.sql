-- One row per email a client asked us to send. This table is the source of truth
-- for an email's status; the queue only carries the email's id.
CREATE TABLE emails (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Idempotency: the client sends the same key when it retries a request.
  -- UNIQUE makes Postgres reject a second row with the same key, even when two
  -- requests arrive at the same moment.
  idempotency_key  text NOT NULL UNIQUE,
  -- Hash of the request body, to catch a key being reused for a different email.
  request_hash     text NOT NULL,

  to_address       text NOT NULL,
  subject          text NOT NULL,
  body             text NOT NULL,
  priority         text NOT NULL CHECK (priority IN ('transactional', 'promotional')),

  status           text NOT NULL DEFAULT 'queued'
                   CHECK (status IN ('queued', 'sending', 'sent', 'failed')),
  attempts         integer NOT NULL DEFAULT 0,
  last_error       text,

  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  sent_at          timestamptz
);

-- Transactional outbox: written in the SAME transaction as the email row.
-- The outbox relay (step 3) reads unpublished rows, publishes them to RabbitMQ,
-- then sets published_at.
CREATE TABLE outbox (
  id            bigserial PRIMARY KEY,
  email_id      uuid NOT NULL REFERENCES emails (id),
  routing_key   text NOT NULL,   -- which queue: 'transactional' or 'promotional'
  payload       jsonb NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  published_at  timestamptz
);

-- The relay only ever looks for unpublished rows, oldest first.
-- A partial index keeps this small: published rows drop out of it.
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
