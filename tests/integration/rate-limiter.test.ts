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
  it('allows a burst up to the capacity, then makes callers wait', async () => {
    const bucket = limiter(10, 3); // 10 tokens/s = one every 100ms

    for (let i = 0; i < 3; i++) expect(await bucket.reserve()).toBe(0);

    const fourth = await bucket.reserve();
    expect(fourth).toBeGreaterThan(0);
    expect(fourth).toBeLessThanOrEqual(100);
  });

  it('queues callers one token-interval apart once the bucket is empty', async () => {
    const bucket = limiter(10, 1); // one token every 100ms
    const waits = [];
    for (let i = 0; i < 4; i++) waits.push(await bucket.reserve());

    // 0, ~100, ~200, ~300: each reservation is for the next free slot
    expect(waits[0]).toBe(0);
    expect(waits[1]).toBeGreaterThan(50);
    expect(waits[2]).toBeGreaterThan(waits[1] + 50);
    expect(waits[3]).toBeGreaterThan(waits[2] + 50);
  });

  it('refills over time', async () => {
    const bucket = limiter(20, 1); // one token every 50ms
    expect(await bucket.reserve()).toBe(0);

    await new Promise((r) => setTimeout(r, 120)); // pays back the debt and refills
    expect(await bucket.reserve()).toBe(0);
  });

  it('never gives more than the capacity for free, even with 50 callers at once', async () => {
    // This is why the logic runs as a Lua script: without atomicity, several callers
    // could all read "1 token left" and all take it.
    const bucket = limiter(1, 10);
    const waits = await Promise.all(Array.from({ length: 50 }, () => bucket.reserve()));

    expect(waits.filter((w) => w === 0)).toHaveLength(10);
    // Everyone else got a different slot: no two callers were promised the same token
    const delayed = waits.filter((w) => w > 0).sort((a, b) => a - b);
    for (let i = 1; i < delayed.length; i++) expect(delayed[i]).toBeGreaterThan(delayed[i - 1]);
  });

  it('is shared: two limiters (two workers) with the same key share one budget', async () => {
    const key = `test:ratelimit:${randomUUID()}`;
    const workerA = new RateLimiter(redis, { key, ratePerSec: 1, capacity: 4 });
    const workerB = new RateLimiter(redis, { key, ratePerSec: 1, capacity: 4 });

    const waits = await Promise.all([
      workerA.reserve(),
      workerB.reserve(),
      workerA.reserve(),
      workerB.reserve(),
      workerA.reserve(),
      workerB.reserve(),
    ]);
    expect(waits.filter((w) => w === 0)).toHaveLength(4);
  });

  it('acquire() sleeps until the reserved token is ready, spreading sends out', async () => {
    const bucket = limiter(20, 1); // one token every 50ms
    const started = Date.now();

    await Promise.all(Array.from({ length: 5 }, () => bucket.acquire()));

    // The first is immediate; the 5th waits for 4 more tokens (~200ms)
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
