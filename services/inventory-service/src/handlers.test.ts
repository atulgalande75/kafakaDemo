import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { pino } from 'pino';
import {
  EventTypes,
  Topics,
  createEvent,
  type OrderCreatedEvent,
  type PaymentFailedEvent,
} from '@orderflow/contracts';
import type { HandlerContext } from '@orderflow/kafka-utils';
import { createInventoryHandlers } from './handlers.js';
import { InventoryStore } from './inventory.js';
import { createTestDb } from './test-db.js';

let db: Awaited<ReturnType<typeof createTestDb>>;
let store: InventoryStore;
const relay = { poke: vi.fn() };
const ctx = { log: pino({ level: 'silent' }) } as unknown as HandlerContext;

beforeAll(async () => {
  db = await createTestDb();
  store = new InventoryStore(db);
});
afterAll(() => db.close());
beforeEach(async () => {
  await db.reset();
  await store.seed([{ sku: 'A', name: 'Alpha', available: 5, lowStockThreshold: 1 }]);
  relay.poke.mockClear();
});

const handlers = () => createInventoryHandlers(store, relay);

const orderCreated = (orderId: string, quantity: number): OrderCreatedEvent =>
  createEvent(
    EventTypes.OrderCreated,
    {
      orderId,
      customerId: 'c',
      items: [{ sku: 'A', quantity, unitPrice: 1 }],
      totalAmount: quantity,
      currency: 'USD',
    },
    { correlationId: 'corr', actor: { sub: 'u1', clientId: 'web' } },
  );

const paymentFailed = (orderId: string): PaymentFailedEvent =>
  createEvent(
    EventTypes.PaymentFailed,
    { orderId, reason: 'card declined' },
    { correlationId: 'corr' },
  );

const topics = async () =>
  (await db.query<{ topic: string }>('SELECT topic FROM outbox ORDER BY id')).rows.map(
    (r) => r.topic,
  );

describe('inventory handlers', () => {
  it('reserves on orders.created and queues the outcome for the relay', async () => {
    await handlers()[Topics.OrdersCreated]!(orderCreated(randomUUID(), 2), ctx);
    expect((await store.get('A'))?.available).toBe(3);
    expect(await topics()).toEqual(['inventory.stock-levels', 'inventory.reserved']);
    expect(relay.poke).toHaveBeenCalled();
  });

  it('queues inventory.rejected for an order it cannot fill', async () => {
    await handlers()[Topics.OrdersCreated]!(orderCreated(randomUUID(), 99), ctx);
    expect(await topics()).toEqual(['inventory.rejected']);
  });

  it('puts stock back when the order payment fails', async () => {
    const orderId = randomUUID();
    await handlers()[Topics.OrdersCreated]!(orderCreated(orderId, 2), ctx);
    await handlers()[Topics.PaymentsFailed]!(paymentFailed(orderId), ctx);
    expect((await store.get('A'))?.available).toBe(5);
    expect(await topics()).toContain('inventory.released');
  });

  it('survives payment failure being processed before the order', async () => {
    const orderId = randomUUID();
    await handlers()[Topics.PaymentsFailed]!(paymentFailed(orderId), ctx);
    await handlers()[Topics.OrdersCreated]!(orderCreated(orderId, 2), ctx);
    expect((await store.get('A'))?.available).toBe(5);
    expect(await topics()).toEqual([]);
  });

  it('is safe to redeliver both events', async () => {
    const orderId = randomUUID();
    const created = orderCreated(orderId, 2);
    const failed = paymentFailed(orderId);
    for (let i = 0; i < 2; i++) {
      await handlers()[Topics.OrdersCreated]!(created, ctx);
      await handlers()[Topics.PaymentsFailed]!(failed, ctx);
    }
    expect((await store.get('A'))?.available).toBe(5);
    expect((await topics()).filter((t) => t === 'inventory.released')).toHaveLength(1);
  });
});
