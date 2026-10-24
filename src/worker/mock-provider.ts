import { EmailProvider } from './email-provider';

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
  ) {}

  async send(): Promise<void> {
    // ±20% jitter, so latency isn't unrealistically identical for every email
    const latency = this.latencyMs * (0.8 + Math.random() * 0.4);
    await new Promise((resolve) => setTimeout(resolve, latency));
    if (Math.random() < this.failureRate) {
      throw new Error('mock provider: simulated temporary failure');
    }
  }
}
