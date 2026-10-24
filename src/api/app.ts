import express, { ErrorRequestHandler } from 'express';
import { Pool } from 'pg';
import { emailsRouter } from './emails.routes';

// The pool is passed in rather than imported, so tests can point the app at a test database.
export function createApp(pool: Pool) {
  const app = express();
  app.use(express.json({ limit: '100kb' }));

  app.get('/health', async (_req, res) => {
    await pool.query('SELECT 1');
    res.json({ status: 'ok' });
  });

  app.use('/v1/emails', emailsRouter(pool));

  // Express 5 sends errors thrown in async handlers here automatically.
  const errorHandler: ErrorRequestHandler = (err, _req, res, _next) => {
    // Body-parser errors (bad JSON, body too large) carry a 4xx status.
    if (typeof err.status === 'number' && err.status < 500) {
      res.status(err.status).json({ error: err.message });
      return;
    }
    console.error(err);
    res.status(500).json({ error: 'Internal server error' });
  };
  app.use(errorHandler);

  return app;
}
