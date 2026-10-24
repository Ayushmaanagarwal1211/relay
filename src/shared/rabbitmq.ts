import amqp, { Channel, ChannelModel, ConfirmChannel, Options } from 'amqplib';

export type Priority = 'transactional' | 'promotional';
export const PRIORITIES: Priority[] = ['transactional', 'promotional'];

// The message we put on the queue. Only the id: the worker reads everything else from Postgres.
export interface EmailMessage {
  emailId: string;
}

/**
 * All RabbitMQ names in one place. `prefix` lets tests use their own isolated queues.
 *
 *   exchange "emails" (direct)
 *     ├─ routing key "transactional" → queue emails.transactional
 *     └─ routing key "promotional"   → queue emails.promotional
 *
 *   retry queues, one per priority per delay, e.g. emails.transactional.retry.10s
 *     Nobody consumes them. A message waits there until its TTL runs out, then RabbitMQ
 *     "dead-letters" it back to the exchange with its original routing key, so it lands
 *     in the main queue again.
 *
 *   emails.dead-letter: emails that failed for good, kept for a human to inspect
 */
export interface Topology {
  exchange: string;
  retryDelaysMs: number[];
  mainQueue(priority: Priority): string;
  retryQueue(priority: Priority, delayMs: number): string;
  deadLetterQueue: string;
}

export function createTopology(retryDelaysMs: number[], prefix = 'emails'): Topology {
  return {
    exchange: prefix,
    retryDelaysMs,
    mainQueue: (priority) => `${prefix}.${priority}`,
    retryQueue: (priority, delayMs) => `${prefix}.${priority}.retry.${formatDelay(delayMs)}`,
    deadLetterQueue: `${prefix}.dead-letter`,
  };
}

// Creates the exchange and queues if they don't exist yet. Safe to call on every startup.
export async function assertTopology(channel: Channel, topology: Topology): Promise<void> {
  await channel.assertExchange(topology.exchange, 'direct', { durable: true });

  for (const priority of PRIORITIES) {
    const main = topology.mainQueue(priority);
    await channel.assertQueue(main, { durable: true });
    await channel.bindQueue(main, topology.exchange, priority);

    // Why one queue per delay instead of one retry queue with a TTL on each message?
    // RabbitMQ only expires messages at the head of a queue. A 5-minute message at the
    // front would block a 10-second message behind it. With one delay per queue,
    // every message in a queue expires in the order it arrived.
    for (const delayMs of topology.retryDelaysMs) {
      await channel.assertQueue(topology.retryQueue(priority, delayMs), {
        durable: true,
        messageTtl: delayMs,
        deadLetterExchange: topology.exchange,
        deadLetterRoutingKey: priority,
      });
    }
  }

  await channel.assertQueue(topology.deadLetterQueue, { durable: true });
}

export async function connectRabbit(url: string): Promise<ChannelModel> {
  return amqp.connect(url);
}

// Publishes directly to a queue and resolves only once RabbitMQ confirms it has the message.
// Without the confirm, a crash right after publishing could lose the message.
export function sendToQueueConfirmed(
  channel: ConfirmChannel,
  queue: string,
  content: Buffer,
  options: Options.Publish,
): Promise<void> {
  return new Promise((resolve, reject) => {
    channel.sendToQueue(queue, content, options, (err) => (err ? reject(err) : resolve()));
  });
}

export function encodeMessage(message: EmailMessage): Buffer {
  return Buffer.from(JSON.stringify(message));
}

export function decodeMessage(content: Buffer): EmailMessage | null {
  try {
    const parsed = JSON.parse(content.toString());
    return typeof parsed?.emailId === 'string' ? { emailId: parsed.emailId } : null;
  } catch {
    return null;
  }
}

function formatDelay(ms: number): string {
  if (ms % 60_000 === 0) return `${ms / 60_000}m`;
  if (ms % 1000 === 0) return `${ms / 1000}s`;
  return `${ms}ms`;
}
