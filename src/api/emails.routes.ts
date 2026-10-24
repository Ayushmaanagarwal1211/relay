import { Router } from 'express';
import { Pool } from 'pg';
import { z } from 'zod';
import { createEmail, getEmail, toEmailResponse } from './emails.service';
import { createEmailSchema, emailIdSchema, idempotencyKeySchema } from './schemas';

export function emailsRouter(pool: Pool): Router {
  const router = Router();

  // POST /v1/emails  (header: Idempotency-Key)
  // 202 Accepted, not 201: the email is queued, not sent yet.
  router.post('/', async (req, res) => {
    const key = idempotencyKeySchema.safeParse(req.get('Idempotency-Key'));
    if (!key.success) {
      res.status(400).json({ error: 'Idempotency-Key header is required (max 255 characters).' });
      return;
    }

    const body = createEmailSchema.safeParse(req.body);
    if (!body.success) {
      res.status(400).json({ error: 'Invalid request body', details: z.flattenError(body.error).fieldErrors });
      return;
    }

    const result = await createEmail(pool, key.data, body.data);

    if (result.kind === 'key_reused') {
      res.status(422).json({ error: 'This Idempotency-Key was already used with a different request body.' });
      return;
    }

    // A retry gets the same response as the original request, plus a header
    // saying so (the same convention Stripe's API uses).
    if (result.kind === 'replayed') res.set('Idempotent-Replayed', 'true');
    res.status(202).json(toEmailResponse(result.email));
  });

  // GET /v1/emails/:id  -> current status of one email
  router.get('/:id', async (req, res) => {
    const id = emailIdSchema.safeParse(req.params.id);
    const email = id.success ? await getEmail(pool, id.data) : null;
    if (!email) {
      res.status(404).json({ error: 'Email not found' });
      return;
    }
    res.json(toEmailResponse(email));
  });

  return router;
}
