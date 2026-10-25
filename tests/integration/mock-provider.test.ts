import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import { createRedis, waitForRedis } from '../../src/shared/rate-limiter';
import { MockProvider, RedisSendRecorder } from '../../src/worker/mock-provider';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379';
let redis: Redis;

beforeAll(async () => {
  redis = createRedis(REDIS_URL);
  await waitForRedis(redis);
  await redis.del(RedisSendRecorder.SENT_KEY, RedisSendRecorder.DUPLICATES_KEY);
});

afterAll(() => {
  redis.disconnect();
});

const email = (id: string) => ({ id, to: 'user@example.com', subject: 'Hi', body: 'Hello' });

describe('MockProvider with a RedisSendRecorder (used by the stress tests)', () => {
  it('records each delivered email once, and counts a second delivery as a duplicate', async () => {
    const provider = new MockProvider(1, 0, new RedisSendRecorder(redis));
    const a = randomUUID();
    const b = randomUUID();

    await provider.send(email(a));
    await provider.send(email(b));
    await provider.send(email(a)); // the same email again: a duplicate

    expect(await redis.sismember(RedisSendRecorder.SENT_KEY, a)).toBe(1);
    expect(await redis.scard(RedisSendRecorder.SENT_KEY)).toBe(2);
    expect(await redis.get(RedisSendRecorder.DUPLICATES_KEY)).toBe('1');
  });

  it("doesn't record a failed send", async () => {
    const provider = new MockProvider(1, 1, new RedisSendRecorder(redis)); // always fails
    const id = randomUUID();

    await expect(provider.send(email(id))).rejects.toThrow('simulated temporary failure');
    expect(await redis.sismember(RedisSendRecorder.SENT_KEY, id)).toBe(0);
  });
});
