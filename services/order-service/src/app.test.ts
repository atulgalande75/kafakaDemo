import { pino } from 'pino';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import { createTestIssuer, type TestIssuer } from '@orderflow/auth/testing';
import { Topics, serializeEvent, type OrderCreatedEvent } from '@orderflow/contracts';
import type { EventProducer } from '@orderflow/kafka-utils';
import { buildApp } from './app.js';
import type { Order } from './order.js';
import { InMemoryOrderRepository } from './repository.js';

let issuer: TestIssuer;
let tokens: { alice: string; bob: string; readOnly: string; writeOnly: string; admin: string };

beforeAll(async () => {
  issuer = await createTestIssuer();
  tokens = {
    alice: await issuer.sign({
      sub: 'alice',
      azp: 'orderflow-cli',
      scope: 'orders:read orders:write',
    }),
    bob: await issuer.sign({ sub: 'bob', azp: 'orderflow-cli', scope: 'orders:read orders:write' }),
    readOnly: await issuer.sign({ sub: 'alice', scope: 'orders:read' }),
    writeOnly: await issuer.sign({ sub: 'alice', scope: 'orders:write' }),
    admin: await issuer.sign({ sub: 'ops', azp: 'dlq-replay', scope: 'admin' }),
  };
});

function setup(publish: EventProducer['publish'] = vi.fn(() => Promise.resolve([]))) {
  const repo = new InMemoryOrderRepository();
  const app = buildApp({
    repo,
    producer: { publish },
    logger: pino({ level: 'silent' }),
    verifier: issuer.verifier,
  });
  const as = (token?: string) => (token ? { authorization: `Bearer ${token}` } : {});
  const createOrder = async (token = tokens.alice) =>
    (
      await app.inject({ method: 'POST', url: '/orders', payload: validOrder, headers: as(token) })
    ).json<Order>();
  return { app, repo, publish, as, createOrder };
}

const validOrder = {
  customerId: 'cust-42',
  customerTier: 'gold',
  country: 'de',
  items: [
    { sku: 'SKU-KEYBOARD', quantity: 1, unitPrice: 79.99 },
    { sku: 'SKU-MOUSE', quantity: 2, unitPrice: 24.5 },
  ],
};

describe('POST /orders', () => {
  it('creates a PENDING order and publishes orders.created keyed by orderId', async () => {
    const { app, publish, as } = setup();
    const res = await app.inject({
      method: 'POST',
      url: '/orders',
      payload: validOrder,
      headers: { ...as(tokens.alice), 'x-correlation-id': 'corr-abc' },
    });

    expect(res.statusCode).toBe(202);
    expect(res.headers['x-correlation-id']).toBe('corr-abc');
    const order = res.json<Order>();
    expect(order).toMatchObject({
      status: 'PENDING',
      totalAmount: 128.99,
      currency: 'USD',
      customerTier: 'gold',
      country: 'DE',
      createdBy: { sub: 'alice', clientId: 'orderflow-cli' },
    });
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

  it('puts the actor (sub + clientId) in the event, never the token', async () => {
    const { app, publish, as } = setup();
    await app.inject({
      method: 'POST',
      url: '/orders',
      payload: validOrder,
      headers: as(tokens.alice),
    });
    const event = vi.mocked(publish).mock.calls[0]![1] as OrderCreatedEvent;
    expect(event.actor).toEqual({ sub: 'alice', clientId: 'orderflow-cli' });
    const wire = serializeEvent(event);
    expect(wire).not.toContain(tokens.alice);
    expect(wire).not.toContain(tokens.alice.split('.')[1]);
  });

  it('401 without a token', async () => {
    const { app, publish } = setup();
    const res = await app.inject({ method: 'POST', url: '/orders', payload: validOrder });
    expect(res.statusCode).toBe(401);
    expect(res.headers['www-authenticate']).toMatch(/^Bearer realm="orderflow"/);
    expect(publish).not.toHaveBeenCalled();
  });

  it('401 with an invalid token', async () => {
    const { app, as } = setup();
    const res = await app.inject({
      method: 'POST',
      url: '/orders',
      payload: validOrder,
      headers: as(await issuer.signWithUnknownKey({ scope: 'orders:write' })),
    });
    expect(res.statusCode).toBe(401);
  });

  it('403 without the orders:write scope', async () => {
    const { app, as, publish } = setup();
    const res = await app.inject({
      method: 'POST',
      url: '/orders',
      payload: validOrder,
      headers: as(tokens.readOnly),
    });
    expect(res.statusCode).toBe(403);
    expect(res.json()).toMatchObject({ error: 'insufficient_scope' });
    expect(res.headers['www-authenticate']).toContain('scope="orders:write"');
    expect(publish).not.toHaveBeenCalled();
  });

  it('400 for invalid orders (after auth)', async () => {
    const { app, as, publish } = setup();
    const res = await app.inject({
      method: 'POST',
      url: '/orders',
      payload: { customerId: '', items: [] },
      headers: as(tokens.alice),
    });
    expect(res.statusCode).toBe(400);
    expect(publish).not.toHaveBeenCalled();
  });

  it('503 and forgets the order when Kafka is unavailable', async () => {
    const { app, repo, as } = setup(() => Promise.reject(new Error('broker down')));
    const res = await app.inject({
      method: 'POST',
      url: '/orders',
      payload: validOrder,
      headers: as(tokens.alice),
    });
    expect(res.statusCode).toBe(503);
    expect(await repo.list(10)).toHaveLength(0);
  });
});

describe('GET /orders/:id', () => {
  it('200 for the owner', async () => {
    const { app, as, createOrder } = setup();
    const order = await createOrder();
    const res = await app.inject({
      method: 'GET',
      url: `/orders/${order.id}`,
      headers: as(tokens.alice),
    });
    expect(res.statusCode).toBe(200);
    expect(res.json<Order>().id).toBe(order.id);
  });

  it('200 for an owner token that only has orders:read', async () => {
    const { app, as, createOrder } = setup();
    const order = await createOrder();
    const res = await app.inject({
      method: 'GET',
      url: `/orders/${order.id}`,
      headers: as(tokens.readOnly),
    });
    expect(res.statusCode).toBe(200);
  });

  it('403 for another user', async () => {
    const { app, as, createOrder } = setup();
    const order = await createOrder(tokens.alice);
    const res = await app.inject({
      method: 'GET',
      url: `/orders/${order.id}`,
      headers: as(tokens.bob),
    });
    expect(res.statusCode).toBe(403);
    expect(res.json()).toMatchObject({ error: 'access_denied' });
  });

  it('200 for admin, whoever owns the order', async () => {
    const { app, as, createOrder } = setup();
    const order = await createOrder(tokens.alice);
    const res = await app.inject({
      method: 'GET',
      url: `/orders/${order.id}`,
      headers: as(tokens.admin),
    });
    expect(res.statusCode).toBe(200);
  });

  it('403 without orders:read (or admin)', async () => {
    const { app, as, createOrder } = setup();
    const order = await createOrder();
    const res = await app.inject({
      method: 'GET',
      url: `/orders/${order.id}`,
      headers: as(tokens.writeOnly),
    });
    expect(res.statusCode).toBe(403);
    expect(res.json()).toMatchObject({ error: 'insufficient_scope' });
  });

  it('401 without a token, 401 with an expired one', async () => {
    const { app, as, createOrder } = setup();
    const order = await createOrder();
    expect((await app.inject({ method: 'GET', url: `/orders/${order.id}` })).statusCode).toBe(401);
    const expired = await issuer.sign(
      { sub: 'alice', scope: 'orders:read' },
      { expiresIn: Math.floor(Date.now() / 1000) - 60 },
    );
    const res = await app.inject({
      method: 'GET',
      url: `/orders/${order.id}`,
      headers: as(expired),
    });
    expect(res.statusCode).toBe(401);
    expect(res.json()).toMatchObject({ message: 'Token expired' });
  });

  it('404 for unknown orders', async () => {
    const { app, as } = setup();
    const res = await app.inject({
      method: 'GET',
      url: '/orders/does-not-exist',
      headers: as(tokens.alice),
    });
    expect(res.statusCode).toBe(404);
  });
});

describe('GET /orders', () => {
  it("lists only the caller's orders, or all of them for admin", async () => {
    const { app, as, createOrder } = setup();
    await createOrder(tokens.alice);
    await createOrder(tokens.alice);
    await createOrder(tokens.bob);

    const list = async (token: string) =>
      (await app.inject({ method: 'GET', url: '/orders', headers: as(token) })).json<Order[]>();
    expect(await list(tokens.alice)).toHaveLength(2);
    expect(await list(tokens.bob)).toHaveLength(1);
    expect(await list(tokens.admin)).toHaveLength(3);
  });
});

describe('POST /orders/:id/republish', () => {
  it('re-sends the identical event (same eventId) for the owner', async () => {
    const { app, as, publish, createOrder } = setup();
    const order = await createOrder();
    const res = await app.inject({
      method: 'POST',
      url: `/orders/${order.id}/republish`,
      headers: as(tokens.alice),
    });
    expect(res.statusCode).toBe(202);
    const [first, second] = vi.mocked(publish).mock.calls.map((c) => c[1] as OrderCreatedEvent);
    expect(second).toEqual(first);
  });

  it("403 for someone else's order", async () => {
    const { app, as, createOrder } = setup();
    const order = await createOrder(tokens.alice);
    const res = await app.inject({
      method: 'POST',
      url: `/orders/${order.id}/republish`,
      headers: as(tokens.bob),
    });
    expect(res.statusCode).toBe(403);
  });
});

describe('GET /health', () => {
  it('is public', async () => {
    const { app } = setup();
    expect((await app.inject({ method: 'GET', url: '/health' })).statusCode).toBe(200);
  });
});
