import { z } from 'zod';

export const createEmailSchema = z.object({
  to: z.email().trim().toLowerCase(),
  subject: z.string().trim().min(1).max(255),
  body: z.string().min(1).max(50_000),
  // transactional = OTPs, password resets (urgent); promotional = newsletters (can wait)
  priority: z.enum(['transactional', 'promotional']).default('transactional'),
});

export type CreateEmailInput = z.infer<typeof createEmailSchema>;

// Clients generate this, usually a UUID, once per email they want sent,
// and send the same value on every retry of that request.
export const idempotencyKeySchema = z.string().trim().min(1).max(255);

export const emailIdSchema = z.uuid();
