import { ChannelModel, ConfirmChannel, ConsumeMessage } from 'amqplib';
import { Pool } from 'pg';
import { assertTopology, decodeMessage, encodeMessage, Priority, sendToQueueConfirmed, Topology } from '../shared/rabbitmq';
import { EmailProvider } from './email-provider';
import { processEmail, ProcessResult } from './process-email';

export interface WorkerOptions {
  pool: Pool;
  connection: ChannelModel;
  topology: Topology;
  provider: EmailProvider;
  priorities: Priority[];
  prefetch: number;
  sendingLeaseMs: number;
}

/**
 * Consumes the main queues and sends each email.
 *
 * Every message is acknowledged (ack) only AFTER we're done with it, including after any
 * retry or dead-letter copy is safely published. If the worker crashes before the ack,
 * RabbitMQ gives the message to another worker. So a message is never lost, only
 * sometimes delivered twice, which processEmail's claim step handles.
 */
export async function startWorker(options: WorkerOptions): Promise<{ stop: () => Promise<void> }> {
  const { pool, connection, topology, provider, priorities, prefetch, sendingLeaseMs } = options;
  const deps = { pool, provider, retryDelaysMs: topology.retryDelaysMs, sendingLeaseMs };

  const setup = await connection.createChannel();
  await assertTopology(setup, topology);
  await setup.close();

  let inFlight = 0;
  let onIdle: (() => void) | null = null;
  const consumers: { channel: ConfirmChannel; tag: string }[] = [];

  // One channel per queue, each with its own prefetch. A backlog of newsletters fills
  // only the promotional channel's slots; transactional emails keep flowing.
  for (const priority of priorities) {
    const channel = await connection.createConfirmChannel();
    // prefetch = at most this many unacknowledged messages at once, i.e. our concurrency limit
    await channel.prefetch(prefetch);

    const handle = async (msg: ConsumeMessage | null) => {
      if (!msg) return; // the queue was deleted
      inFlight++;
      try {
        await handleMessage(channel, msg, priority);
      } finally {
        inFlight--;
        if (inFlight === 0) onIdle?.();
      }
    };

    const { consumerTag } = await channel.consume(topology.mainQueue(priority), (msg) => void handle(msg));
    consumers.push({ channel, tag: consumerTag });
    console.log(`[worker] consuming ${topology.mainQueue(priority)} (prefetch ${prefetch})`);
  }

  async function handleMessage(channel: ConfirmChannel, msg: ConsumeMessage, priority: Priority) {
    const message = decodeMessage(msg.content);
    if (!message) {
      // Can't even read it: retrying won't help. Park it in the dead-letter queue.
      await sendToQueueConfirmed(channel, topology.deadLetterQueue, msg.content, {
        persistent: true,
        headers: { 'x-error': 'malformed message', 'x-source-queue': topology.mainQueue(priority) },
      });
      channel.ack(msg);
      return;
    }

    let result: ProcessResult;
    try {
      result = await processEmail(deps, message.emailId);
    } catch (err) {
      // Infrastructure problem (e.g. Postgres is down), not a problem with this email.
      // Put the message back on the queue after a pause, so we don't spin in a tight loop.
      console.error(`[worker] ${message.emailId}: unexpected error, requeueing in 1s:`, err);
      await new Promise((r) => setTimeout(r, 1000));
      channel.nack(msg, false, true);
      return;
    }

    const id = message.emailId;
    const content = encodeMessage(message);
    const headers = { 'x-email-id': id };

    switch (result.outcome) {
      case 'sent':
        console.log(`[worker] ${id}: sent via ${provider.name} (attempt ${result.attempts})`);
        break;
      case 'skipped':
        console.log(`[worker] ${id}: skipped (${result.reason}), probably a duplicate message`);
        break;
      case 'busy':
        // Another worker holds this email. Check again after the shortest retry delay:
        // by then it's either sent (we'll skip it) or that worker's lease has expired.
        await sendToQueueConfirmed(channel, topology.retryQueue(priority, topology.retryDelaysMs[0]), content, {
          persistent: true,
          headers,
        });
        console.log(`[worker] ${id}: another worker is sending it, checking again later`);
        break;
      case 'retry':
        await sendToQueueConfirmed(channel, topology.retryQueue(priority, result.delayMs), content, {
          persistent: true,
          headers: { ...headers, 'x-last-error': result.error },
        });
        console.warn(`[worker] ${id}: attempt ${result.attempts} failed (${result.error}), retrying in ${result.delayMs}ms`);
        break;
      case 'dead-letter':
        await sendToQueueConfirmed(channel, topology.deadLetterQueue, content, {
          persistent: true,
          headers: { ...headers, 'x-error': result.error, 'x-reason': result.reason, 'x-attempts': result.attempts },
        });
        console.error(`[worker] ${id}: gave up after ${result.attempts} attempt(s) (${result.reason}): ${result.error}`);
        break;
    }

    // Only now is it safe to remove the original message from the queue.
    channel.ack(msg);
  }

  return {
    async stop() {
      // Graceful shutdown: stop taking new messages, finish the ones in progress, then close.
      for (const { channel, tag } of consumers) await channel.cancel(tag);
      if (inFlight > 0) await new Promise<void>((resolve) => (onIdle = resolve));
      for (const { channel } of consumers) await channel.close();
    },
  };
}
