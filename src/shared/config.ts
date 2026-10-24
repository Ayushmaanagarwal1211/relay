import { z } from 'zod';

// Load .env if it exists. Variables already set in the shell win.
try {
  process.loadEnvFile();
} catch {
  // No .env file: rely on the shell environment.
}

const commaSeparatedNumbers = z
  .string()
  .transform((s) => s.split(',').map((part) => Number(part.trim())))
  .pipe(z.array(z.number().int().positive()).min(1));

const schema = z.object({
  PORT: z.coerce.number().int().positive().default(3000),
  DATABASE_URL: z.string().min(1),
  RABBITMQ_URL: z.string().min(1).default('amqp://relay:relay@localhost:5672'),

  SMTP_HOST: z.string().min(1).default('localhost'),
  SMTP_PORT: z.coerce.number().int().positive().default(1025),
  EMAIL_FROM: z.string().min(1).default('Relay <no-reply@relay.local>'),

  // Outbox relay: how often to check for new rows, and how many to publish at once
  RELAY_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(500),
  RELAY_BATCH_SIZE: z.coerce.number().int().positive().default(100),

  // Worker: wait before each retry. 3 delays = 1 first try + 3 retries = 4 attempts max.
  RETRY_DELAYS_MS: commaSeparatedNumbers.default([10_000, 60_000, 300_000]),
  // Max emails one worker handles at the same time, per queue
  WORKER_PREFETCH: z.coerce.number().int().positive().default(10),
  // If an email has been 'sending' longer than this, assume that worker crashed
  SENDING_LEASE_MS: z.coerce.number().int().positive().default(60_000),
  // Which queues this worker process consumes, e.g. "transactional" to run an OTP-only worker
  WORKER_QUEUES: z
    .string()
    .default('transactional,promotional')
    .transform((s) => s.split(',').map((q) => q.trim()))
    .pipe(z.array(z.enum(['transactional', 'promotional'])).min(1)),
});

// Fail fast at startup if config is missing, instead of failing on the first request.
const parsed = schema.safeParse(process.env);
if (!parsed.success) {
  console.error('Invalid environment variables:', z.flattenError(parsed.error).fieldErrors);
  process.exit(1);
}

export const config = parsed.data;
