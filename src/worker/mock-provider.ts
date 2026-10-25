import Redis from 'ioredis';
import { EmailProvider, OutgoingEmail } from './email-provider';

// Remembers which emails the mock "delivered", so a stress test can check for lost and duplicate emails.
export interface SendRecorder {
  record(emailId: string): Promise<void>;
}

/**
 * Stores every delivered email id in a Redis set. SADD returns 0 when the id is already
 * in the set, meaning the provider got the same email twice: count it as a duplicate.
 * After the test: SCARD = unique emails delivered, the counter = duplicate sends.
 */
export class RedisSendRecorder implements SendRecorder {
  static readonly SENT_KEY = 'mock:sent';
  static readonly DUPLICATES_KEY = 'mock:duplicates';

  constructor(private redis: Redis) {}

  async record(emailId: string): Promise<void> {
    const added = await this.redis.sadd(RedisSendRecorder.SENT_KEY, emailId);
    if (added === 0) await this.redis.incr(RedisSendRecorder.DUPLICATES_KEY);
  }
}

/**
 * A fake email provider for load tests. It sends nothing; it just waits like a real
 * provider would (network round trip) and fails a chosen fraction of the time.
 *
 * Why not load-test against Mailpit or a real provider? We want to measure OUR system
 * (API, outbox, RabbitMQ, workers), not someone else's server or their rate limits.
 */
export class MockProvider implements EmailProvider {
  readonly name = 'mock';

  constructor(
    private latencyMs: number,
    private failureRate: number, // 0 = never fails, 0.1 = 10% of sends fail temporarily
    private recorder?: SendRecorder,
  ) {}

  async send(email: OutgoingEmail): Promise<void> {
    // ±20% jitter, so latency isn't unrealistically identical for every email
    const latency = this.latencyMs * (0.8 + Math.random() * 0.4);
    await new Promise((resolve) => setTimeout(resolve, latency));
    if (Math.random() < this.failureRate) {
      throw new Error('mock provider: simulated temporary failure');
    }
    // The provider has the email now. If the worker crashes before saving 'sent',
    // it will be sent again later, and the recorder will count that duplicate.
    await this.recorder?.record(email.id);
  }
}
