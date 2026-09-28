import { randomUUID } from 'node:crypto';
import { pino } from 'pino';
import { describe, expect, it, vi } from 'vitest';
import { EventTypes, Topics, createEvent, type AnyEvent } from '@orderflow/contracts';
import type { EventProducer, HandlerContext } from '@orderflow/kafka-utils';
import { createPaymentHandlers } from './handlers.js';
import { PaymentGatewayError, chargeCard, type PaymentSettings } from './payment.js';

const settings: PaymentSettings = { failureRate: 0, declineRate: 0, cardLimit: 2000 };
const orderData = (totalAmount = 50) => ({
  orderId: randomUUID(),
  customerId: 'c-1',
  items: [{ sku: 'SKU-MOUSE', quantity: 2, unitPrice: totalAmount / 2 }],
  totalAmount,
  currency: 'USD',
});
const ctx = { log: pino({ level: 'silent' }), attempt: 1 } as HandlerContext;

describe('chargeCard', () => {
  it('approves normal payments', () => {
    expect(chargeCard(orderData(), settings)).toEqual({ approved: true });
  });

  it('declines amounts above the card limit', () => {
    expect(chargeCard(orderData(5000), settings)).toMatchObject({
      approved: false,
      reason: expect.stringContaining('exceeds card limit') as unknown,
    });
  });

  it('declines randomly at PAYMENT_DECLINE_RATE', () => {
    expect(chargeCard(orderData(), { ...settings, declineRate: 0.5 }, () => 0.4)).toEqual({
      approved: false,
      reason: 'Card declined by issuer',
    });
  });

  it('throws a transient error at PAYMENT_FAILURE_RATE (chaos)', () => {
    expect(() => chargeCard(orderData(), { ...settings, failureRate: 1 })).toThrow(
      PaymentGatewayError,
    );
  });
});

describe('payment handler', () => {
  function run(data: ReturnType<typeof orderData>, s = settings) {
    const published: Array<{ topic: string; event: AnyEvent; key: string }> = [];
    const publish: EventProducer['publish'] = (topic, event, { key }) => {
      published.push({ topic, event: event as AnyEvent, key });
      return Promise.resolve([]);
    };
    const producer = { publish: vi.fn(publish) };
    const source = createEvent(EventTypes.OrderCreated, data, { correlationId: 'corr-9' });
    const handler = createPaymentHandlers(producer, s)[Topics.OrdersCreated]!;
    return { published, source, done: handler(source, ctx) };
  }

  it('publishes payments.completed with the same correlationId, keyed by orderId', async () => {
    const data = orderData();
    const { published, done } = run(data);
    await done;
    expect(published).toHaveLength(1);
    expect(published[0]).toMatchObject({
      topic: 'payments.completed',
      key: data.orderId,
      event: { type: 'payment.completed', correlationId: 'corr-9', data: { amount: 50 } },
    });
  });

  it('publishes payments.failed when declined', async () => {
    const { published, done } = run(orderData(9999));
    await done;
    expect(published[0]?.topic).toBe('payments.failed');
  });

  it('derives the outcome eventId from the source event (stable across retries)', async () => {
    const first = run(orderData());
    await first.done;
    const handler = createPaymentHandlers(
      {
        publish: (_topic, event) => {
          expect(event.eventId).toBe(first.published[0]?.event.eventId);
          return Promise.resolve([]);
        },
      },
      settings,
    )[Topics.OrdersCreated]!;
    await handler(first.source, ctx);
  });
});
