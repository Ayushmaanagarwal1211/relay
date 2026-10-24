import { decideOnFailure } from '../../src/worker/retry-policy';

const delays = [10_000, 60_000, 300_000];

describe('decideOnFailure', () => {
  it('waits longer after each failed attempt', () => {
    expect(decideOnFailure(1, delays, false)).toEqual({ action: 'retry', delayMs: 10_000 });
    expect(decideOnFailure(2, delays, false)).toEqual({ action: 'retry', delayMs: 60_000 });
    expect(decideOnFailure(3, delays, false)).toEqual({ action: 'retry', delayMs: 300_000 });
  });

  it('gives up once every retry is used (3 delays = 4 attempts)', () => {
    expect(decideOnFailure(4, delays, false)).toEqual({ action: 'dead-letter', reason: 'max-attempts' });
  });

  it('gives up immediately on a permanent error, even on the first attempt', () => {
    expect(decideOnFailure(1, delays, true)).toEqual({ action: 'dead-letter', reason: 'permanent-error' });
  });
});
