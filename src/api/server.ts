import { config } from '../shared/config';
import { createPool } from '../shared/db';
import { createApp } from './app';

const pool = createPool(config.DATABASE_URL);
const server = createApp(pool).listen(config.PORT, () => {
  console.log(`API listening on http://localhost:${config.PORT}`);
});

// Graceful shutdown: stop accepting requests, let in-flight ones finish, then close DB connections.
function shutdown(signal: string) {
  console.log(`${signal} received, shutting down...`);
  server.close(() => {
    pool.end().then(() => process.exit(0));
  });
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
