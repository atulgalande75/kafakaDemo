import type { IHeaders, Kafka, Message, RecordMetadata } from 'kafkajs';
import { Partitioners } from 'kafkajs';
import type { Logger } from 'pino';
import { Headers, serializeEvent, type Envelope } from '@orderflow/contracts';

export interface PublishOptions {
  /** Partition key. All events for one order use the orderId so they stay ordered. */
  key: string;
  headers?: IHeaders;
}

export interface EventProducer {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  /** Publishes a validated event envelope with standard headers. */
  publish(topic: string, event: Envelope, options: PublishOptions): Promise<RecordMetadata[]>;
  /** Publishes raw messages as-is (used for DLQ and replay). */
  sendRaw(topic: string, messages: Message[]): Promise<RecordMetadata[]>;
}

export function eventHeaders(event: Envelope): IHeaders {
  return {
    [Headers.EventId]: event.eventId,
    [Headers.EventType]: event.type,
    [Headers.EventVersion]: String(event.version),
    [Headers.CorrelationId]: event.correlationId,
  };
}

export function createEventProducer(kafka: Kafka, logger: Logger): EventProducer {
  const producer = kafka.producer({
    // Idempotent producer: broker de-duplicates retried sends (requires acks=all, 1 in-flight).
    idempotent: true,
    maxInFlightRequests: 1,
    // Java-compatible murmur2 partitioning: same key -> same partition across clients.
    createPartitioner: Partitioners.DefaultPartitioner,
    retry: { retries: 5 },
  });

  const sendRaw = (topic: string, messages: Message[]) =>
    producer.send({ topic, messages, acks: -1 });

  return {
    async connect() {
      await producer.connect();
      logger.info('producer connected');
    },
    async disconnect() {
      await producer.disconnect();
    },
    async publish(topic, event, { key, headers }) {
      const metadata = await sendRaw(topic, [
        { key, value: serializeEvent(event), headers: { ...eventHeaders(event), ...headers } },
      ]);
      logger.debug(
        { topic, key, eventId: event.eventId, partition: metadata[0]?.partition },
        'published event',
      );
      return metadata;
    },
    sendRaw,
  };
}
