import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import { createRedis, RateLimiter, waitForRedis } from '../../src/shared/rate-limiter';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
let redis: Redis;

beforeAll(async () => {
  redis = createRedis(REDIS_URL);
  await waitForRedis(redis);
});

afterAll(() => {
  redis.disconnect();
});

// A fresh bucket per test, so tests don't share tokens
const limiter = (ratePerSec: number, capacity: number) =>
  new RateLimiter(redis, { key: `test:ratelimit:${randomUUID()}`, ratePerSec, capacity });

describe('RateLimiter (token bucket in Redis)', () => {
  it('allows a burst up to the capacity, then says how long to wait', async () => {
    const bucket = limiter(10, 3);

    for (let i = 0; i < 3; i++) expect((await bucket.tryTake()).allowed).toBe(true);

    const fourth = await bucket.tryTake();
    expect(fourth.allowed).toBe(false);
    // 10 tokens/s = one token every 100ms
    expect(fourth.waitMs).toBeGreaterThan(0);
    expect(fourth.waitMs).toBeLessThanOrEqual(100);
  });

  it('refills over time', async () => {
    const bucket = limiter(20, 1); // one token every 50ms
    expect((await bucket.tryTake()).allowed).toBe(true);
    expect((await bucket.tryTake()).allowed).toBe(false);

    await new Promise((r) => setTimeout(r, 80));
    expect((await bucket.tryTake()).allowed).toBe(true);
  });

  it('never hands out more tokens than it has, even with 50 callers at once', async () => {
    // This is why the logic runs as a Lua script: without atomicity, several callers
    // could all read "1 token left" and all take it.
    const bucket = limiter(1, 10); // refills 1/s, so the test is effectively frozen in time
    const results = await Promise.all(Array.from({ length: 50 }, () => bucket.tryTake()));
    expect(results.filter((r) => r.allowed)).toHaveLength(10);
  });

  it('is shared: two limiters (two workers) with the same key share one budget', async () => {
    const key = `test:ratelimit:${randomUUID()}`;
    const workerA = new RateLimiter(redis, { key, ratePerSec: 1, capacity: 4 });
    const workerB = new RateLimiter(redis, { key, ratePerSec: 1, capacity: 4 });

    const results = await Promise.all([
      workerA.tryTake(),
      workerB.tryTake(),
      workerA.tryTake(),
      workerB.tryTake(),
      workerA.tryTake(),
      workerB.tryTake(),
    ]);
    expect(results.filter((r) => r.allowed)).toHaveLength(4);
  });

  it('acquire() waits until a token is free, which spreads sends out over time', async () => {
    const bucket = limiter(20, 1); // one token every 50ms
    const started = Date.now();

    for (let i = 0; i < 5; i++) await bucket.acquire();

    // The first is immediate; the next 4 each wait ~50ms
    expect(Date.now() - started).toBeGreaterThanOrEqual(180);
  });

  it('fails open: if Redis is unreachable, acquire() returns at once instead of blocking', async () => {
    const deadRedis = createRedis('redis://localhost:1'); // nothing listens on port 1
    const bucket = new RateLimiter(deadRedis, { key: 'test:unreachable', ratePerSec: 1, capacity: 1 });
    const started = Date.now();

    await bucket.acquire();
    await bucket.acquire();

    expect(Date.now() - started).toBeLessThan(1000);
    deadRedis.disconnect();
  });
});
