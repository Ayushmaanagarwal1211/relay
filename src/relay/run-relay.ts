import { ChannelModel, ConfirmChannel } from 'amqplib';
import { Pool } from 'pg';
import { log } from '../shared/log';
import { assertTopology, Topology } from '../shared/rabbitmq';
import { Publisher, relayBatch } from './outbox-relay';

export interface RelayOptions {
  pool: Pool;
  connection: ChannelModel;
  topology: Topology;
  pollIntervalMs: number;
  batchSize: number;
}

function rabbitPublisher(channel: ConfirmChannel, exchange: string): Publisher {
  return {
    publish(routingKey, payload, messageId) {
      channel.publish(exchange, routingKey, Buffer.from(JSON.stringify(payload)), {
        persistent: true, // RabbitMQ writes it to disk, so it survives a broker restart
        contentType: 'application/json',
        messageId,
      });
    },
    waitForConfirms: () => channel.waitForConfirms(),
  };
}

// Polls the outbox in a loop until stop() is called.
export async function startRelay(options: RelayOptions): Promise<{ stop: () => Promise<void> }> {
  const { pool, connection, topology, pollIntervalMs, batchSize } = options;
  const channel = await connection.createConfirmChannel();
  await assertTopology(channel, topology);
  const publisher = rabbitPublisher(channel, topology.exchange);

  let stopping = false;
  let wakeUp: (() => void) | null = null;

  const loop = (async () => {
    while (!stopping) {
      let published = 0;
      try {
        published = await relayBatch(pool, publisher, batchSize);
        if (published > 0) log.info(`[relay] published ${published} message(s)`);
      } catch (err) {
        log.error('[relay] batch failed, will retry:', err);
      }
      // A full batch means there are probably more rows waiting: go again straight away.
      // Otherwise sleep until the next poll (or until stop() wakes us up).
      if (published < batchSize && !stopping) {
        await new Promise<void>((resolve) => {
          wakeUp = resolve;
          setTimeout(resolve, pollIntervalMs);
        });
      }
    }
  })();

  return {
    async stop() {
      stopping = true;
      wakeUp?.();
      await loop; // let the current batch finish
      await channel.close();
    },
  };
}
