import cluster from 'node:cluster';
import { config } from '../shared/config';
import { createPool } from '../shared/db';
import { createApp } from './app';

/**
 * Node runs JavaScript on ONE CPU core per process. On an 8-core machine, one API process
 * would leave 7 cores idle. With API_PROCESSES > 1 we use Node's cluster module:
 *
 *   primary process (doesn't handle requests)
 *     ├─ API process 1 ─┐
 *     ├─ API process 2 ─┼─ all listen on the same port; the OS spreads connections
 *     └─ API process N ─┘
 *
 * The processes share nothing in memory, which is fine: all state lives in Postgres.
 * (That's why the API can scale this way at all: it's stateless.)
 */
if (config.API_PROCESSES > 1 && cluster.isPrimary) {
  runPrimary(config.API_PROCESSES);
} else {
  runServer();
}

function runPrimary(processes: number) {
  console.log(`API: starting ${processes} processes`);
  for (let i = 0; i < processes; i++) cluster.fork();

  let shuttingDown = false;
  // If an API process crashes, start a new one, like Kubernetes would restart a pod
  cluster.on('exit', (worker, code, signal) => {
    if (shuttingDown) {
      if (Object.keys(cluster.workers ?? {}).length === 0) process.exit(0);
      return;
    }
    console.error(`API process ${worker.process.pid} died (${signal ?? code}), starting a new one`);
    cluster.fork();
  });

  const shutdown = (signal: string) => {
    console.log(`${signal} received, stopping API processes...`);
    shuttingDown = true;
    for (const worker of Object.values(cluster.workers ?? {})) worker?.process.kill('SIGTERM');
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

function runServer() {
  const pool = createPool(config.DATABASE_URL, config.DB_POOL_SIZE);
  const server = createApp(pool).listen(config.PORT, () => {
    if (cluster.isPrimary) console.log(`API listening on http://localhost:${config.PORT}`);
  });

  // Graceful shutdown: stop accepting requests, let in-flight ones finish, then close DB connections.
  let closing = false;
  function shutdown(signal: string) {
    if (closing) return;
    closing = true;
    if (cluster.isPrimary) console.log(`${signal} received, shutting down...`);
    server.close(() => {
      pool.end().then(() => process.exit(0));
    });
  }
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}
