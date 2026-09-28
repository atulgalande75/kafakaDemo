import { pino } from 'pino';
import { describe, expect, it, vi } from 'vitest';
import { Topics, type OrderCreatedEvent } from '@orderflow/contracts';
import type { EventProducer } from '@orderflow/kafka-utils';
import { buildApp } from './app.js';
import type { Order } from './order.js';
import { InMemoryOrderRepository } from './repository.js';

function setup(publish: EventProducer['publish'] = vi.fn(() => Promise.resolve([]))) {
  const repo = new InMemoryOrderRepository();
  const app = buildApp({ repo, producer: { publish }, logger: pino({ level: 'silent' }) });
  return { app, repo, publish };
}

const validOrder = {
  customerId: 'cust-42',
  items: [
    { sku: 'SKU-KEYBOARD', quantity: 1, unitPrice: 79.99 },
    { sku: 'SKU-MOUSE', quantity: 2, unitPrice: 24.5 },
  ],
};

describe('order-service HTTP API', () => {
  it('POST /orders creates a PENDING order and publishes orders.created keyed by orderId', async () => {
    const { app, publish } = setup();
    const res = await app.inject({
      method: 'POST',
      url: '/orders',
      payload: validOrder,
      headers: { 'x-correlation-id': 'corr-abc' },
    });

    expect(res.statusCode).toBe(202);
    expect(res.headers['x-correlation-id']).toBe('corr-abc');
    const order = res.json<Order>();
    expect(order).toMatchObject({ status: 'PENDING', totalAmount: 128.99, currency: 'USD' });
    expect(res.headers.location).toBe(`/orders/${order.id}`);

    expect(publish).toHaveBeenCalledWith(
      Topics.OrdersCreated,
      expect.objectContaining({
        type: 'order.created',
        correlationId: 'corr-abc',
        data: expect.objectContaining({ orderId: order.id }) as unknown,
      }),
      { key: order.id },
    );
  });

  it('GET /orders/:id returns the order, 404 when unknown', async () => {
    const { app } = setup();
    const created = (
      await app.inject({ method: 'POST', url: '/orders', payload: validOrder })
    ).json<Order>();

    const found = await app.inject({ method: 'GET', url: `/orders/${created.id}` });
    expect(found.statusCode).toBe(200);
    expect(found.json<Order>().id).toBe(created.id);

    const missing = await app.inject({ method: 'GET', url: '/orders/does-not-exist' });
    expect(missing.statusCode).toBe(404);
  });

  it('rejects invalid orders with 400', async () => {
    const { app, publish } = setup();
    const res = await app.inject({
      method: 'POST',
      url: '/orders',
      payload: { customerId: '', items: [] },
    });
    expect(res.statusCode).toBe(400);
    expect(publish).not.toHaveBeenCalled();
  });

  it('returns 503 and forgets the order when Kafka is unavailable', async () => {
    const { app, repo } = setup(() => Promise.reject(new Error('broker down')));
    const res = await app.inject({ method: 'POST', url: '/orders', payload: validOrder });
    expect(res.statusCode).toBe(503);
    expect(await repo.list(10)).toHaveLength(0);
  });

  it('POST /orders/:id/republish re-sends the identical event (same eventId)', async () => {
    const { app, publish } = setup();
    const order = (
      await app.inject({ method: 'POST', url: '/orders', payload: validOrder })
    ).json<Order>();
    const res = await app.inject({ method: 'POST', url: `/orders/${order.id}/republish` });

    expect(res.statusCode).toBe(202);
    const calls = vi.mocked(publish).mock.calls;
    const [first, second] = calls.map((c) => c[1] as OrderCreatedEvent);
    expect(second).toEqual(first);
  });
});
