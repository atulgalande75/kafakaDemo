import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import {
  EventTypes,
  InvalidEventError,
  TOPIC_SPECS,
  Topics,
  createEvent,
  decodeEvent,
  deriveEventId,
  dlqTopic,
  serializeEvent,
  sourceTopicOf,
} from './index.js';

const orderData = () => ({
  orderId: randomUUID(),
  customerId: 'cust-1',
  customerTier: 'standard' as const,
  country: 'US',
  items: [{ sku: 'SKU-KEYBOARD', quantity: 2, unitPrice: 49.5 }],
  totalAmount: 99,
  currency: 'USD',
});

describe('topics', () => {
  it('derives DLQ topic names', () => {
    expect(dlqTopic(Topics.OrdersCreated)).toBe('orders.created.dlq');
    expect(sourceTopicOf('orders.created.dlq')).toBe('orders.created');
    expect(() => sourceTopicOf('orders.created')).toThrow();
  });

  it('declares every business topic with 3 partitions plus a DLQ', () => {
    expect(TOPIC_SPECS.find((t) => t.topic === 'orders.created')?.numPartitions).toBe(3);
    expect(TOPIC_SPECS.map((t) => t.topic)).toContain('payments.failed.dlq');
    expect(TOPIC_SPECS).toHaveLength(10);
  });
});

describe('createEvent', () => {
  it('builds a complete envelope', () => {
    const event = createEvent(EventTypes.OrderCreated, orderData(), { correlationId: 'corr-1' });
    expect(event).toMatchObject({ type: 'order.created', version: 1, correlationId: 'corr-1' });
    expect(event.eventId).toMatch(/^[0-9a-f-]{36}$/);
    expect(new Date(event.occurredAt).toString()).not.toBe('Invalid Date');
  });

  it('rejects invalid payloads at the producer', () => {
    expect(() =>
      createEvent(EventTypes.OrderCreated, { ...orderData(), items: [] }, { correlationId: 'c' }),
    ).toThrow();
  });
});

describe('decodeEvent', () => {
  it('round-trips a valid event', () => {
    const event = createEvent(EventTypes.OrderCreated, orderData(), { correlationId: 'c' });
    expect(decodeEvent(Topics.OrdersCreated, Buffer.from(serializeEvent(event)))).toEqual(event);
  });

  it.each([
    ['empty value', null],
    ['non-JSON', 'not json {'],
    ['missing envelope fields', JSON.stringify({ hello: 'world' })],
  ])('rejects %s', (_label, value) => {
    expect(() => decodeEvent(Topics.OrdersCreated, value)).toThrow(InvalidEventError);
  });

  it('rejects an event of the wrong type for the topic', () => {
    const event = createEvent(
      EventTypes.PaymentFailed,
      { orderId: randomUUID(), reason: 'declined' },
      { correlationId: 'c' },
    );
    expect(() => decodeEvent(Topics.OrdersCreated, serializeEvent(event))).toThrow(
      /Unexpected event type "payment.failed"/,
    );
  });

  it('rejects unsupported versions', () => {
    const event = createEvent(EventTypes.OrderCreated, orderData(), { correlationId: 'c' });
    expect(() =>
      decodeEvent(Topics.OrdersCreated, JSON.stringify({ ...event, version: 2 })),
    ).toThrow(/Unsupported version 2/);
  });

  it('rejects an invalid payload with a readable message', () => {
    const event = createEvent(EventTypes.OrderCreated, orderData(), { correlationId: 'c' });
    const bad = { ...event, data: { ...event.data, items: [{ sku: 'X', quantity: -1 }] } };
    expect(() => decodeEvent(Topics.OrdersCreated, JSON.stringify(bad))).toThrow(
      /Invalid "order.created" payload/,
    );
  });
});

describe('deriveEventId', () => {
  it('is deterministic, distinct per name and a valid envelope id', () => {
    const source = randomUUID();
    const id = deriveEventId(source, 'payment');
    expect(deriveEventId(source, 'payment')).toBe(id);
    expect(deriveEventId(source, 'inventory')).not.toBe(id);
    expect(() =>
      createEvent(EventTypes.OrderCreated, orderData(), { correlationId: 'c', eventId: id }),
    ).not.toThrow();
  });
});

describe('actor', () => {
  it('carries only sub and clientId', () => {
    const actor = { sub: 'user-1', clientId: 'orderflow-cli' };
    const event = createEvent(EventTypes.OrderCreated, orderData(), { correlationId: 'c', actor });
    expect(event.actor).toEqual(actor);
  });

  it('never copies extra fields such as a token into the event', () => {
    const actor = { sub: 'user-1', clientId: 'cli', token: 'eyJhbGciOi.secret.sig' };
    const event = createEvent(EventTypes.OrderCreated, orderData(), { correlationId: 'c', actor });
    expect(serializeEvent(event)).not.toContain('eyJhbGciOi');
  });

  it('rejects envelopes whose actor has unknown fields', () => {
    const event = createEvent(EventTypes.OrderCreated, orderData(), { correlationId: 'c' });
    const withToken = { ...event, actor: { sub: 's', clientId: 'c', accessToken: 'x' } };
    expect(() => decodeEvent(Topics.OrdersCreated, JSON.stringify(withToken))).toThrow(
      InvalidEventError,
    );
  });

  it('still accepts events without an actor or the newer order fields', () => {
    const event = createEvent(EventTypes.OrderCreated, orderData(), { correlationId: 'c' });
    const { customerTier, country, ...legacyData } = event.data;
    const decoded = decodeEvent(
      Topics.OrdersCreated,
      JSON.stringify({ ...event, data: legacyData }),
    );
    expect(decoded.actor).toBeUndefined();
    expect(decoded.data).toMatchObject({ customerTier: 'standard', country: 'US' });
  });
});
