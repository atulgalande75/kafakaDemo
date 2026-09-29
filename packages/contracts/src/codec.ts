import { createHash, randomUUID } from 'node:crypto';
import { z } from 'zod';
import { envelopeSchema, type Actor } from './envelope.js';
import {
  eventSchemas,
  topicEventType,
  type DataInputOf,
  type EventForTopic,
  type EventOf,
  type EventType,
} from './events.js';
import type { Topic } from './topics.js';

/** Thrown when a message cannot be decoded into a valid event. Never worth retrying. */
export class InvalidEventError extends Error {
  override readonly name = 'InvalidEventError';
  constructor(
    message: string,
    readonly details?: string,
  ) {
    super(details ? `${message}: ${details}` : message);
  }
}

export interface CreateEventOptions {
  correlationId: string;
  /** Who initiated the flow; copy it from the causing event when reacting to one. */
  actor?: Actor;
  eventId?: string;
  occurredAt?: Date;
}

/**
 * Deterministic event id (UUID v5-style) derived from the event that caused it.
 * If a consumer processes the same input twice (retry, redelivery), the events it
 * emits get the *same* eventId, so downstream consumers can de-duplicate them too.
 */
export function deriveEventId(causationEventId: string, name: string): string {
  const bytes = createHash('sha1').update(`${causationEventId}/${name}`).digest().subarray(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x50; // version 5
  bytes[8] = (bytes[8]! & 0x3f) | 0x80; // RFC 4122 variant
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/** Creates a new, schema-validated event envelope. */
export function createEvent<T extends EventType>(
  type: T,
  data: DataInputOf<T>,
  options: CreateEventOptions,
): EventOf<T> {
  const candidate = {
    eventId: options.eventId ?? randomUUID(),
    type,
    version: 1,
    occurredAt: (options.occurredAt ?? new Date()).toISOString(),
    correlationId: options.correlationId,
    ...(options.actor && { actor: { sub: options.actor.sub, clientId: options.actor.clientId } }),
    data,
  };
  return eventSchemas[type].parse(candidate) as EventOf<T>;
}

export function serializeEvent(event: { eventId: string }): string {
  return JSON.stringify(event);
}

/**
 * Decodes and validates a raw Kafka message value for a topic. Rejects malformed
 * JSON, envelopes that don't match, events of the wrong type for the topic and
 * unsupported versions - all with an {@link InvalidEventError}.
 */
export function decodeEvent<T extends Topic>(
  topic: T,
  value: Buffer | string | null | undefined,
): EventForTopic<T> {
  if (value == null || value.length === 0) {
    throw new InvalidEventError('Empty message value');
  }

  let json: unknown;
  try {
    json = JSON.parse(value.toString());
  } catch (err) {
    // JSON.parse quotes the raw input; strip control/binary bytes so logs stay readable.
    // eslint-disable-next-line no-control-regex -- deliberately matching control bytes
    const details = (err as Error).message.replace(/[\u0000-\u001f\u007f-\u009f\ufffd]/g, '?');
    throw new InvalidEventError('Message value is not valid JSON', details);
  }

  const envelope = envelopeSchema.safeParse(json);
  if (!envelope.success) {
    throw new InvalidEventError('Invalid event envelope', z.prettifyError(envelope.error));
  }

  const expectedType = topicEventType[topic];
  if (envelope.data.type !== expectedType) {
    throw new InvalidEventError(
      `Unexpected event type "${envelope.data.type}" on topic "${topic}" (expected "${expectedType}")`,
    );
  }

  const schema = eventSchemas[expectedType];
  const expectedVersion = schema.shape.version.value;
  if (envelope.data.version !== expectedVersion) {
    throw new InvalidEventError(
      `Unsupported version ${envelope.data.version} for "${expectedType}" (supported: ${expectedVersion})`,
    );
  }

  const event = schema.safeParse(json);
  if (!event.success) {
    throw new InvalidEventError(`Invalid "${expectedType}" payload`, z.prettifyError(event.error));
  }
  return event.data as EventForTopic<T>;
}
