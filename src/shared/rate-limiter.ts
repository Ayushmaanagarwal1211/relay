import Redis from 'ioredis';
import { log } from './log';

/**
 * Token bucket, stored in Redis so that EVERY worker shares the same limit.
 *
 *   - The bucket holds up to `capacity` tokens (the burst size).
 *   - It refills at `ratePerSec` tokens per second.
 *   - Each send takes 1 token. No token? Wait until one refills.
 *
 * Example: ratePerSec = 100, capacity = 100. After a quiet period, 100 emails can go out
 * at once (a burst), then about 100 per second after that, however many workers you run.
 *
 * Why Redis and not a counter in each worker's memory? With 3 workers each allowed
 * 100/s, you'd really send 300/s. The limit has to live in one shared place.
 *
 * Why a Lua script? Read the bucket, refill, take a token, save: if two workers did
 * these steps separately, both could read "1 token left" and both take it. Redis runs a
 * Lua script as one atomic step; nothing else runs in between.
 *
 * The script uses Redis's own clock (TIME), not the worker's, so workers whose clocks
 * disagree can't refill the bucket too fast.
 */
const TAKE_TOKEN_LUA = `
local capacity = tonumber(ARGV[1])
local rate_per_ms = tonumber(ARGV[2]) / 1000

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'updated_at')
local tokens = tonumber(bucket[1]) or capacity
local updated_at = tonumber(bucket[2]) or now

-- Refill for the time that passed since the last call, up to the capacity
tokens = math.min(capacity, tokens + math.max(0, now - updated_at) * rate_per_ms)

local allowed = 0
local wait_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  wait_ms = math.ceil((1 - tokens) / rate_per_ms)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated_at', now)
-- Forget idle buckets: once it would be full again anyway, the key isn't needed
redis.call('PEXPIRE', KEYS[1], math.ceil(capacity / rate_per_ms) + 1000)

return {allowed, wait_ms}
`;

type RedisWithBucket = Redis & {
  takeToken(key: string, capacity: number, ratePerSec: number): Promise<[number, number]>;
};

export interface RateLimiterOptions {
  key: string; // e.g. "ratelimit:provider:smtp"
  ratePerSec: number;
  capacity: number;
}

export class RateLimiter {
  private redis: RedisWithBucket;
  private lastWarning = 0;

  constructor(
    redis: Redis,
    private options: RateLimiterOptions,
  ) {
    redis.defineCommand('takeToken', { numberOfKeys: 1, lua: TAKE_TOKEN_LUA });
    this.redis = redis as RedisWithBucket;
  }

  // One attempt: { allowed: true } or { allowed: false, waitMs: how long until a token is ready }.
  async tryTake(): Promise<{ allowed: boolean; waitMs: number }> {
    const { key, capacity, ratePerSec } = this.options;
    const [allowed, waitMs] = await this.redis.takeToken(key, capacity, ratePerSec);
    return { allowed: allowed === 1, waitMs };
  }

  /**
   * Waits until a token is available, then takes it. Returns how long it waited (ms).
   *
   * If Redis is unreachable, we FAIL OPEN: send without limiting, and log a warning.
   * Trade-off: we might briefly exceed the provider's limit (it would then answer
   * "slow down", a temporary error we already retry), but OTPs keep flowing.
   * Failing closed would stop ALL email whenever Redis has a problem.
   */
  async acquire(): Promise<number> {
    const started = Date.now();
    for (;;) {
      let result: { allowed: boolean; waitMs: number };
      try {
        result = await this.tryTake();
      } catch (err) {
        this.warnThrottled(err);
        return Date.now() - started;
      }
      if (result.allowed) return Date.now() - started;
      // Random jitter, so waiting workers don't all retry at the same instant
      await sleep(result.waitMs + Math.random() * 10);
    }
  }

  private warnThrottled(err: unknown) {
    if (Date.now() - this.lastWarning < 10_000) return; // at most one warning per 10s
    this.lastWarning = Date.now();
    log.warn(`[rate-limit] Redis unavailable, sending without rate limiting: ${(err as Error).message}`);
  }
}

// A Redis connection that fails fast instead of queueing commands while disconnected,
// so a Redis outage can't make the workers hang.
export function createRedis(url: string): Redis {
  const redis = new Redis(url, { enableOfflineQueue: false, maxRetriesPerRequest: 1, commandTimeout: 500 });
  redis.on('error', () => {
    // Connection errors are reported through acquire(); ioredis keeps reconnecting.
  });
  return redis;
}

// Resolves once connected, or after timeoutMs either way (we fail open, so don't block startup).
export function waitForRedis(redis: Redis, timeoutMs = 5000): Promise<void> {
  if (redis.status === 'ready') return Promise.resolve();
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, timeoutMs);
    redis.once('ready', () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
