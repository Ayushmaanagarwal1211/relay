import { createHash } from 'node:crypto';
import type { CreateEmailInput } from './schemas';

// Fingerprint of a request body. If a client reuses an idempotency key with a
// different body, the hashes differ and we reject it instead of silently
// returning the old email.
export function hashRequest(input: CreateEmailInput): string {
  // Fixed field order, so the same email always gives the same hash.
  const canonical = JSON.stringify([input.to, input.subject, input.body, input.priority]);
  return createHash('sha256').update(canonical).digest('hex');
}
