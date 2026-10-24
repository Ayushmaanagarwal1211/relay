import { config } from '../shared/config';
import { createPool } from '../shared/db';
import { connectRabbit, createTopology } from '../shared/rabbitmq';
import { createRedis, RateLimiter, waitForRedis } from '../shared/rate-limiter';
import { EmailProvider, SmtpProvider } from './email-provider';
import { MockProvider } from './mock-provider';
import { startWorker } from './run-worker';

function createProvider(): EmailProvider {
  if (config.EMAIL_PROVIDER === 'mock') {
    console.log(
      `[worker] using MOCK provider (${config.MOCK_LATENCY_MS}ms latency, ${config.MOCK_FAILURE_RATE * 100}% failures)`,
    );
    return new MockProvider(config.MOCK_LATENCY_MS, config.MOCK_FAILURE_RATE);
  }
  return new SmtpProvider({ host: config.SMTP_HOST, port: config.SMTP_PORT }, config.EMAIL_FROM);
}

async function main() {
  const pool = createPool(config.DATABASE_URL);
  const connection = await connectRabbit(config.RABBITMQ_URL);

  // If RabbitMQ goes away, exit and let the process manager restart us. Messages we hadn't
  // acknowledged go back on the queue automatically, so another worker picks them up.
  connection.on('close', () => {
    console.error('[worker] RabbitMQ connection closed, exiting');
    process.exit(1);
  });

  const provider = createProvider();
  const redis = createRedis(config.REDIS_URL);
  await waitForRedis(redis);
  const ratePerSec = config.PROVIDER_RATE_LIMIT_PER_SEC;
  const throttle =
    ratePerSec > 0
      ? new RateLimiter(redis, {
          key: `ratelimit:provider:${provider.name}`,
          ratePerSec,
          capacity: config.PROVIDER_RATE_LIMIT_BURST ?? Math.max(1, Math.ceil(ratePerSec)),
        })
      : undefined;
  console.log(
    throttle ? `[worker] rate limit: ${ratePerSec} emails/s to ${provider.name}` : '[worker] rate limiting is off',
  );

  const worker = await startWorker({
    pool,
    connection,
    topology: createTopology(config.RETRY_DELAYS_MS),
    provider,
    throttle,
    priorities: config.WORKER_QUEUES,
    prefetch: config.WORKER_PREFETCH,
    sendingLeaseMs: config.SENDING_LEASE_MS,
  });

  const shutdown = async () => {
    console.log('[worker] shutting down, finishing in-flight emails...');
    connection.removeAllListeners('close');
    await worker.stop();
    await connection.close();
    redis.disconnect();
    await pool.end();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('[worker] failed to start:', err);
  process.exit(1);
});
