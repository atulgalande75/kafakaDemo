import type { Consumer, IHeaders, Kafka, KafkaMessage, Message } from 'kafkajs';
import type { Logger } from 'pino';
import {
  decodeEvent,
  dlqTopic,
  type DlqRecord,
  type EventForTopic,
  type Topic,
} from '@orderflow/contracts';
import { backoffDelay, DEFAULT_RETRY_POLICY, sleep, type RetryPolicy } from './backoff.js';
import { isRetryable, toError } from './errors.js';
import { InMemoryIdempotencyStore, type IdempotencyStore } from './idempotency.js';
import type { EventProducer } from './producer.js';

export interface HandlerContext {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  /** 1 on the first try, 2 on the first retry, ... */
  attempt: number;
  /** Logger bound to eventId, correlationId, topic, partition and offset. */
  log: Logger;
}

export type EventHandler<E> = (event: E, ctx: HandlerContext) => Promise<void>;

/** One handler per subscribed topic, typed by the event that topic carries. */
export type EventHandlers = { [T in Topic]?: EventHandler<EventForTopic<T>> };

export type ProcessOutcome = 'processed' | 'duplicate' | 'dead-lettered';

export interface ProcessDeps {
  groupId: string;
  handlers: EventHandlers;
  retry: RetryPolicy;
  idempotency: IdempotencyStore;
  producer: Pick<EventProducer, 'sendRaw'>;
  logger: Logger;
  sleep?: (ms: number) => Promise<void>;
  heartbeat?: () => Promise<void>;
  random?: () => number;
  now?: () => Date;
}

export interface IncomingMessage {
  topic: string;
  partition: number;
  message: KafkaMessage;
}

/**
 * Processes one Kafka message:
 *
 * 1. decode + validate           -> invalid messages go straight to `<topic>.dlq`
 * 2. idempotency check (eventId) -> already-processed events are skipped
 * 3. run the handler             -> transient failures are retried with exponential
 *                                   backoff; after `maxRetries` (or on a
 *                                   NonRetryableError) the message goes to the DLQ
 * 4. record the eventId as processed
 *
 * Resolves once the message is fully handled (so its offset can be committed).
 * Rejects only if the DLQ itself can't be written, in which case the offset is
 * not committed and Kafka redelivers the message - nothing is silently dropped.
 */
export async function processMessage(
  deps: ProcessDeps,
  { topic, partition, message }: IncomingMessage,
): Promise<ProcessOutcome> {
  const wait = deps.sleep ?? ((ms: number) => sleep(ms));
  const baseLog = deps.logger.child({ topic, partition, offset: message.offset });

  const handler = deps.handlers[topic as Topic] as EventHandler<unknown> | undefined;
  if (!handler) throw new Error(`No handler registered for topic "${topic}"`);

  let event: EventForTopic<Topic>;
  try {
    event = decodeEvent(topic as Topic, message.value);
  } catch (err) {
    const error = toError(err);
    baseLog.error({ err: error }, 'invalid message -> DLQ');
    await publishToDlq(deps, { topic, partition, message }, error, 'invalid-message', 0);
    return 'dead-lettered';
  }

  const log = baseLog.child({ eventId: event.eventId, correlationId: event.correlationId });

  if (await deps.idempotency.has(event.eventId)) {
    log.info({ type: event.type }, 'duplicate event skipped (already processed)');
    return 'duplicate';
  }

  const maxAttempts = deps.retry.maxRetries + 1;
  for (let attempt = 1; ; attempt++) {
    try {
      await handler(event, {
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString() ?? null,
        attempt,
        log,
      });
      await deps.idempotency.add(event.eventId);
      return 'processed';
    } catch (err) {
      const error = toError(err);
      if (!isRetryable(error) || attempt >= maxAttempts) {
        log.error(
          { err: error, attempt, retryable: isRetryable(error) },
          'handler failed permanently -> DLQ',
        );
        await publishToDlq(
          deps,
          { topic, partition, message },
          error,
          'processing-failed',
          attempt,
        );
        return 'dead-lettered';
      }
      const delay = backoffDelay(attempt, deps.retry, deps.random);
      log.warn(
        { attempt, maxAttempts, retryInMs: delay, error: error.message },
        'handler failed, retrying',
      );
      await deps.heartbeat?.();
      await wait(delay);
      await deps.heartbeat?.();
    }
  }
}

export function headersToRecord(headers: IHeaders | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [name, value] of Object.entries(headers ?? {})) {
    if (value === undefined) continue;
    out[name] = Array.isArray(value) ? value.map(String).join(',') : value.toString();
  }
  return out;
}

function encodeValue(value: Buffer | null): Pick<DlqRecord['original'], 'value' | 'valueEncoding'> {
  if (value === null) return { value: null, valueEncoding: 'utf8' };
  const text = value.toString('utf8');
  return Buffer.from(text, 'utf8').equals(value)
    ? { value: text, valueEncoding: 'utf8' }
    : { value: value.toString('base64'), valueEncoding: 'base64' };
}

export function buildDlqMessage(
  { topic, partition, message }: IncomingMessage,
  error: Error,
  reason: DlqRecord['reason'],
  attempts: number,
  groupId: string,
  now: Date = new Date(),
): Message {
  const originalHeaders = headersToRecord(message.headers);
  const record: DlqRecord = {
    reason,
    error: { name: error.name, message: error.message, stack: error.stack },
    attempts,
    failedAt: now.toISOString(),
    consumerGroup: groupId,
    original: {
      topic,
      partition,
      offset: message.offset,
      timestamp: message.timestamp,
      key: message.key?.toString() ?? null,
      headers: originalHeaders,
      ...encodeValue(message.value),
    },
  };
  return {
    key: message.key,
    value: JSON.stringify(record),
    headers: {
      ...originalHeaders,
      'dlq-reason': reason,
      'dlq-error': error.message.slice(0, 500),
      'dlq-consumer-group': groupId,
      'dlq-original-topic': topic,
    },
  };
}

async function publishToDlq(
  deps: ProcessDeps,
  incoming: IncomingMessage,
  error: Error,
  reason: DlqRecord['reason'],
  attempts: number,
): Promise<void> {
  const target = dlqTopic(incoming.topic);
  const message = buildDlqMessage(
    incoming,
    error,
    reason,
    attempts,
    deps.groupId,
    deps.now?.() ?? new Date(),
  );
  await deps.producer.sendRaw(target, [message]);
  deps.logger.warn(
    { dlq: target, reason, originalOffset: incoming.message.offset, partition: incoming.partition },
    'message published to DLQ',
  );
}

export interface EventConsumerOptions {
  kafka: Kafka;
  groupId: string;
  handlers: EventHandlers;
  /** Used to publish to DLQ topics. */
  producer: EventProducer;
  logger: Logger;
  retry?: RetryPolicy;
  idempotency?: IdempotencyStore;
  /** Where a brand-new consumer group starts. Default: earliest, so nothing is missed. */
  fromBeginning?: boolean;
}

export interface EventConsumer {
  start(): Promise<void>;
  stop(): Promise<void>;
}

/** Consumer-group wrapper around {@link processMessage}. */
export function createEventConsumer(options: EventConsumerOptions): EventConsumer {
  const { kafka, groupId, handlers, producer, fromBeginning = true } = options;
  const logger = options.logger.child({ groupId });
  const consumer: Consumer = kafka.consumer({ groupId, sessionTimeout: 30_000 });
  const stopping = new AbortController();
  const topics = Object.keys(handlers);

  const deps: ProcessDeps = {
    groupId,
    handlers,
    producer,
    logger,
    retry: options.retry ?? DEFAULT_RETRY_POLICY,
    idempotency: options.idempotency ?? new InMemoryIdempotencyStore(),
    // Abort backoff sleeps on shutdown: the message is left uncommitted and redelivered later.
    sleep: (ms) => sleep(ms, stopping.signal),
  };

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    logger.info(
      { memberId: payload.memberId, assignment: payload.memberAssignment },
      'joined consumer group',
    );
  });
  consumer.on(consumer.events.CRASH, ({ payload }) => {
    logger.error({ err: payload.error, restart: payload.restart }, 'consumer crashed');
  });

  return {
    async start() {
      await consumer.connect();
      await consumer.subscribe({ topics, fromBeginning });
      await consumer.run({
        eachMessage: async (payload) => {
          const { topic, partition, message } = payload;
          await processMessage(
            { ...deps, heartbeat: () => payload.heartbeat() },
            { topic, partition, message },
          );
        },
      });
      logger.info({ topics }, 'consumer started');
    },
    async stop() {
      stopping.abort();
      await consumer.disconnect();
    },
  };
}
