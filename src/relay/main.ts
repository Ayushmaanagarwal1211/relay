import { config } from '../shared/config';
import { createPool } from '../shared/db';
import { connectRabbit, createTopology } from '../shared/rabbitmq';
import { startRelay } from './run-relay';

async function main() {
  const pool = createPool(config.DATABASE_URL);
  const connection = await connectRabbit(config.RABBITMQ_URL);

  // If RabbitMQ goes away, exit and let the process manager (Docker, Kubernetes) restart us.
  // Nothing is lost: unpublished rows stay in the outbox until we're back.
  connection.on('close', () => {
    console.error('[relay] RabbitMQ connection closed, exiting');
    process.exit(1);
  });

  const relay = await startRelay({
    pool,
    connection,
    topology: createTopology(config.RETRY_DELAYS_MS),
    pollIntervalMs: config.RELAY_POLL_INTERVAL_MS,
    batchSize: config.RELAY_BATCH_SIZE,
  });
  console.log(`[relay] polling the outbox every ${config.RELAY_POLL_INTERVAL_MS}ms`);

  const shutdown = async () => {
    console.log('[relay] shutting down...');
    connection.removeAllListeners('close');
    await relay.stop();
    await connection.close();
    await pool.end();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('[relay] failed to start:', err);
  process.exit(1);
});
