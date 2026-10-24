import Redis from 'ioredis';
import { log } from './log';

/**
 * Token bucket, stored in Redis so that EVERY worker shares the same limit.
 *
 *   - The bucket holds up to `capacity` tokens (the burst size).
 *   - It refills at `ratePerSec` tokens per second.
 *   - Each send takes 1 token.
 *
 * Example: ratePerSec = 100, capacity = 100. After a quiet period, 100 emails can go out
 * at once (a burst), then about 100 per second after that, however many workers you run.
 *
 * RESERVATIONS: when the bucket is empty, the caller still takes a token, "on credit":
 * the bucket goes below zero, and Redis answers "your token is ready in X ms". The caller
 * sleeps exactly X ms and sends. One Redis call per email, however busy it gets.
 * (The first version made callers ask again and again "is there a token yet?". Under
 * load, 40 waiting callers asking every ~10ms meant thousands of Redis calls a second,
 * which slowed everything else on the machine. The load test showed it.)
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
const RESERVE_TOKEN_LUA = `
local capacity = tonumber(ARGV[1])
local rate_per_ms = tonumber(ARGV[2]) / 1000

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local bucket = redis.call('HMGET', KEYS[1], 'tokens', 'updated_at')
local tokens = tonumber(bucket[1]) or capacity
local updated_at = tonumber(bucket[2]) or now

-- Refill for the time that passed since the last call, up to the capacity
tokens = math.min(capacity, tokens + math.max(0, now - updated_at) * rate_per_ms)

-- Take one token. Below zero means "reserved": it will exist once enough time passes.
tokens = tokens - 1
local wait_ms = 0
if tokens < 0 then
  wait_ms = math.ceil(-tokens / rate_per_ms)
end

redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated_at', now)
-- Forget the key once the bucket would be full again anyway
redis.call('PEXPIRE', KEYS[1], math.ceil((capacity - tokens) / rate_per_ms) + 1000)

return wait_ms
`;

type RedisWithBucket = Redis & {
  reserveToken(key: string, capacity: number, ratePerSec: number): Promise<number>;
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
    redis.defineCommand('reserveToken', { numberOfKeys: 1, lua: RESERVE_TOKEN_LUA });
    this.redis = redis as RedisWithBucket;
  }

  // Takes one token and returns how many ms until it's ready (0 = send now).
  async reserve(): Promise<number> {
    const { key, capacity, ratePerSec } = this.options;
    return this.redis.reserveToken(key, capacity, ratePerSec);
  }

  /**
   * Waits until our token is ready. Returns how long it waited (ms).
   *
   * If Redis is unreachable, we FAIL OPEN: send without limiting, and log a warning.
   * Trade-off: we might briefly exceed the provider's limit (it would then answer
   * "slow down", a temporary error we already retry), but OTPs keep flowing.
   * Failing closed would stop ALL email whenever Redis has a problem.
   */
  async acquire(): Promise<number> {
    let waitMs: number;
    try {
      waitMs = await this.reserve();
    } catch (err) {
      this.warnThrottled(err);
      return 0;
    }
    if (waitMs > 0) await sleep(waitMs);
    return waitMs;
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
