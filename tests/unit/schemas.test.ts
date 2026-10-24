import { createEmailSchema, idempotencyKeySchema } from '../../src/api/schemas';

describe('createEmailSchema', () => {
  const valid = { to: 'User@Example.com', subject: 'Hello', body: 'Hi there' };

  it('accepts a valid email and defaults priority to transactional', () => {
    const result = createEmailSchema.parse(valid);
    expect(result.priority).toBe('transactional');
  });

  it('lowercases the address, so the same person always gets the same hash', () => {
    expect(createEmailSchema.parse(valid).to).toBe('user@example.com');
  });

  it.each([
    ['an invalid address', { ...valid, to: 'not-an-email' }],
    ['an empty subject', { ...valid, subject: '   ' }],
    ['a missing body', { to: valid.to, subject: valid.subject }],
    ['an unknown priority', { ...valid, priority: 'urgent' }],
  ])('rejects %s', (_label, input) => {
    expect(createEmailSchema.safeParse(input).success).toBe(false);
  });
});

describe('idempotencyKeySchema', () => {
  it('rejects a missing or empty key', () => {
    expect(idempotencyKeySchema.safeParse(undefined).success).toBe(false);
    expect(idempotencyKeySchema.safeParse('').success).toBe(false);
  });

  it('rejects keys longer than 255 characters', () => {
    expect(idempotencyKeySchema.safeParse('k'.repeat(256)).success).toBe(false);
  });
});
