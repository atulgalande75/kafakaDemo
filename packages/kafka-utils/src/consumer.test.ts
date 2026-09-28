import { randomUUID } from 'node:crypto';
import type { KafkaMessage, Message } from 'kafkajs';
import { pino } from 'pino';
import { describe, expect, it, vi } from 'vitest';
import {
  EventTypes,
  Topics,
  createEvent,
  dlqRecordSchema,
  serializeEvent,
} from '@orderflow/contracts';
import { NonRetryableError } from './errors.js';
import { InMemoryIdempotencyStore } from './idempotency.js';
import { processMessage, type EventHandlers, type ProcessDeps } from './consumer.js';

const orderCreated = () =>
  createEvent(
    EventTypes.OrderCreated,
    {
      orderId: randomUUID(),
      customerId: 'cust-1',
      items: [{ sku: 'SKU-MOUSE', quantity: 1, unitPrice: 25 }],
      totalAmount: 25,
      currency: 'USD',
    },
    { correlationId: 'corr-1' },
  );

const kafkaMessage = (value: string | Buffer | null, offset = '42'): KafkaMessage => ({
  key: Buffer.from('order-key'),
  value: typeof value === 'string' ? Buffer.from(value) : value,
  offset,
  timestamp: '1700000000000',
  attributes: 0,
  headers: { 'correlation-id': 'corr-1' },
});

function setup(handlers: EventHandlers, overrides: Partial<ProcessDeps> = {}) {
  const sent: Array<{ topic: string; messages: Message[] }> = [];
  const deps: ProcessDeps = {
    groupId: 'test-group',
    handlers,
    retry: { maxRetries: 2, initialDelayMs: 100, maxDelayMs: 1000, multiplier: 2, jitter: 0 },
    idempotency: new InMemoryIdempotencyStore(),
    producer: {
      sendRaw: (topic, messages) => {
        sent.push({ topic, messages });
        return Promise.resolve([]);
      },
    },
    logger: pino({ level: 'silent' }),
    sleep: vi.fn(() => Promise.resolve()),
    now: () => new Date('2024-01-01T00:00:00.000Z'),
    ...overrides,
  };
  const dlqRecords = () =>
    sent.flatMap((s) => s.messages.map((m) => dlqRecordSchema.parse(JSON.parse(String(m.value)))));
  return { deps, sent, dlqRecords };
}

describe('processMessage', () => {
  it('passes a decoded, typed event to the handler', async () => {
    const event = orderCreated();
    const handler = vi.fn(() => Promise.resolve());
    const { deps, sent } = setup({ [Topics.OrdersCreated]: handler });

    const outcome = await processMessage(deps, {
      topic: Topics.OrdersCreated,
      partition: 1,
      message: kafkaMessage(serializeEvent(event)),
    });

    expect(outcome).toBe('processed');
    expect(handler).toHaveBeenCalledWith(
      event,
      expect.objectContaining({ partition: 1, offset: '42', key: 'order-key', attempt: 1 }),
    );
    expect(sent).toHaveLength(0);
  });

  it('skips events whose eventId was already processed', async () => {
    const event = orderCreated();
    const handler = vi.fn(() => Promise.resolve());
    const { deps } = setup({ [Topics.OrdersCreated]: handler });
    const incoming = {
      topic: Topics.OrdersCreated,
      partition: 0,
      message: kafkaMessage(serializeEvent(event)),
    };

    expect(await processMessage(deps, incoming)).toBe('processed');
    expect(
      await processMessage(deps, {
        ...incoming,
        message: kafkaMessage(serializeEvent(event), '43'),
      }),
    ).toBe('duplicate');
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('sends invalid messages straight to the DLQ without calling the handler', async () => {
    const handler = vi.fn(() => Promise.resolve());
    const { deps, sent, dlqRecords } = setup({ [Topics.OrdersCreated]: handler });

    const outcome = await processMessage(deps, {
      topic: Topics.OrdersCreated,
      partition: 2,
      message: kafkaMessage('{"this is": "not an event"'),
    });

    expect(outcome).toBe('dead-lettered');
    expect(handler).not.toHaveBeenCalled();
    expect(sent[0]?.topic).toBe('orders.created.dlq');
    expect(dlqRecords()[0]).toMatchObject({
      reason: 'invalid-message',
      attempts: 0,
      consumerGroup: 'test-group',
      error: { name: 'InvalidEventError' },
      original: {
        topic: 'orders.created',
        partition: 2,
        offset: '42',
        key: 'order-key',
        value: '{"this is": "not an event"',
        valueEncoding: 'utf8',
        headers: { 'correlation-id': 'corr-1' },
      },
    });
  });

  it('base64-encodes binary poison messages', async () => {
    const { deps, dlqRecords } = setup({ [Topics.OrdersCreated]: () => Promise.resolve() });
    const binary = Buffer.from([0xff, 0xfe, 0x00, 0x81]);
    await processMessage(deps, {
      topic: Topics.OrdersCreated,
      partition: 0,
      message: kafkaMessage(binary),
    });
    expect(dlqRecords()[0]?.original).toMatchObject({
      valueEncoding: 'base64',
      value: binary.toString('base64'),
    });
  });

  it('retries transient failures with exponential backoff, then succeeds', async () => {
    const handler = vi
      .fn()
      .mockRejectedValueOnce(new Error('db timeout'))
      .mockRejectedValueOnce(new Error('db timeout'))
      .mockResolvedValue(undefined);
    const { deps, sent } = setup({ [Topics.OrdersCreated]: handler });

    const outcome = await processMessage(deps, {
      topic: Topics.OrdersCreated,
      partition: 0,
      message: kafkaMessage(serializeEvent(orderCreated())),
    });

    expect(outcome).toBe('processed');
    expect(handler).toHaveBeenCalledTimes(3);
    expect(deps.sleep).toHaveBeenNthCalledWith(1, 100);
    expect(deps.sleep).toHaveBeenNthCalledWith(2, 200);
    expect(sent).toHaveLength(0);
  });

  it('dead-letters after exhausting retries and does not mark the event processed', async () => {
    const event = orderCreated();
    const handler = vi.fn().mockRejectedValue(new Error('payment gateway unavailable'));
    const { deps, dlqRecords } = setup({ [Topics.OrdersCreated]: handler });

    const outcome = await processMessage(deps, {
      topic: Topics.OrdersCreated,
      partition: 0,
      message: kafkaMessage(serializeEvent(event)),
    });

    expect(outcome).toBe('dead-lettered');
    expect(handler).toHaveBeenCalledTimes(3); // 1 attempt + 2 retries
    expect(dlqRecords()[0]).toMatchObject({
      reason: 'processing-failed',
      attempts: 3,
      error: { message: 'payment gateway unavailable' },
      failedAt: '2024-01-01T00:00:00.000Z',
    });
    // A replay from the DLQ must be processed again.
    expect(await deps.idempotency.has(event.eventId)).toBe(false);
  });

  it('does not retry NonRetryableError', async () => {
    const handler = vi.fn().mockRejectedValue(new NonRetryableError('unknown sku'));
    const { deps, dlqRecords } = setup({ [Topics.OrdersCreated]: handler });

    await processMessage(deps, {
      topic: Topics.OrdersCreated,
      partition: 0,
      message: kafkaMessage(serializeEvent(orderCreated())),
    });

    expect(handler).toHaveBeenCalledTimes(1);
    expect(deps.sleep).not.toHaveBeenCalled();
    expect(dlqRecords()[0]).toMatchObject({ attempts: 1, error: { name: 'NonRetryableError' } });
  });

  it('rejects (so the offset is not committed) when the DLQ cannot be written', async () => {
    const { deps } = setup(
      { [Topics.OrdersCreated]: () => Promise.resolve() },
      { producer: { sendRaw: () => Promise.reject(new Error('broker down')) } },
    );
    await expect(
      processMessage(deps, {
        topic: Topics.OrdersCreated,
        partition: 0,
        message: kafkaMessage('garbage'),
      }),
    ).rejects.toThrow('broker down');
  });
});
