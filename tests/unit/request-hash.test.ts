import { hashRequest } from '../../src/api/request-hash';
import type { CreateEmailInput } from '../../src/api/schemas';

const email: CreateEmailInput = {
  to: 'user@example.com',
  subject: 'Your OTP',
  body: 'Your code is 123456',
  priority: 'transactional',
};

describe('hashRequest', () => {
  it('gives the same hash for the same email', () => {
    expect(hashRequest({ ...email })).toBe(hashRequest(email));
  });

  it('gives a different hash when any field changes', () => {
    const base = hashRequest(email);
    expect(hashRequest({ ...email, to: 'other@example.com' })).not.toBe(base);
    expect(hashRequest({ ...email, subject: 'Changed' })).not.toBe(base);
    expect(hashRequest({ ...email, body: 'Your code is 999999' })).not.toBe(base);
    expect(hashRequest({ ...email, priority: 'promotional' })).not.toBe(base);
  });

  it('does not confuse values that join to the same text', () => {
    // Naive string joining would make these two identical: "ab" + "c" vs "a" + "bc"
    const a = hashRequest({ ...email, subject: 'ab', body: 'c' });
    const b = hashRequest({ ...email, subject: 'a', body: 'bc' });
    expect(a).not.toBe(b);
  });
});
