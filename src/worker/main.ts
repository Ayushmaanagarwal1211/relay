import { config } from '../shared/config';
import { createPool } from '../shared/db';
import { connectRabbit, createTopology } from '../shared/rabbitmq';
import { SmtpProvider } from './email-provider';
import { startWorker } from './run-worker';

async function main() {
  const pool = createPool(config.DATABASE_URL);
  const connection = await connectRabbit(config.RABBITMQ_URL);

  // If RabbitMQ goes away, exit and let the process manager restart us. Messages we hadn't
  // acknowledged go back on the queue automatically, so another worker picks them up.
  connection.on('close', () => {
    console.error('[worker] RabbitMQ connection closed, exiting');
    process.exit(1);
  });

  const worker = await startWorker({
    pool,
    connection,
    topology: createTopology(config.RETRY_DELAYS_MS),
    provider: new SmtpProvider({ host: config.SMTP_HOST, port: config.SMTP_PORT }, config.EMAIL_FROM),
    priorities: config.WORKER_QUEUES,
    prefetch: config.WORKER_PREFETCH,
    sendingLeaseMs: config.SENDING_LEASE_MS,
  });

  const shutdown = async () => {
    console.log('[worker] shutting down, finishing in-flight emails...');
    connection.removeAllListeners('close');
    await worker.stop();
    await connection.close();
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
