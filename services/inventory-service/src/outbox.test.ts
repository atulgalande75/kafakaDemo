import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { pino } from 'pino';
import { InventoryStore } from './inventory.js';
import { OutboxRelay } from './outbox.js';
import { createTestDb } from './test-db.js';

let db: Awaited<ReturnType<typeof createTestDb>>;
let store: InventoryStore;

const logger = pino({ level: 'silent' });

beforeAll(async () => {
  db = await createTestDb();
  store = new InventoryStore(db);
});
afterAll(() => db.close());
beforeEach(async () => {
  await db.reset();
  await store.seed([{ sku: 'A', name: 'Alpha', available: 50, lowStockThreshold: 5 }]);
  for (let i = 0; i < 3; i++) {
    await store.reserve(randomUUID(), [{ sku: 'A', quantity: 1 }], { correlationId: `c-${i}` });
  }
});

const published = () =>
  db.query<{ n: string }>('SELECT count(*) n FROM outbox WHERE published_at IS NOT NULL');

describe('OutboxRelay', () => {
  it('publishes pending rows in insertion order and marks them published', async () => {
    const sent: Array<{ topic: string; key: string }> = [];
    const producer = {
      publish: vi.fn((topic: string, _e: unknown, o: { key: string }) => {
        sent.push({ topic, key: o.key });
        return Promise.resolve([]);
      }),
    };
    const relay = new OutboxRelay(db, producer, logger);

    expect(await relay.pending()).toBe(6); // 3 x (stock level + reservation)
    expect(await relay.drain()).toBe(6);
    expect(sent.map((s) => s.topic)).toEqual([
      'inventory.stock-levels',
      'inventory.reserved',
      'inventory.stock-levels',
      'inventory.reserved',
      'inventory.stock-levels',
      'inventory.reserved',
    ]);
    expect(await relay.pending()).toBe(0);
    expect(await relay.drain()).toBe(0);
  });

  it('keeps going from the failed row after Kafka recovers, without resending earlier rows', async () => {
    let calls = 0;
    const sent: string[] = [];
    const producer = {
      publish: vi.fn((_t: string, e: { eventId: string }) => {
        if (++calls === 3) return Promise.reject(new Error('broker down'));
        sent.push(e.eventId);
        return Promise.resolve([]);
      }),
    };
    const relay = new OutboxRelay(db, producer, logger);

    await expect(relay.drain()).rejects.toThrow('broker down');
    expect(Number((await published()).rows[0]!.n)).toBe(2); // the two before the failure
    expect(await relay.pending()).toBe(4);

    expect(await relay.drain()).toBe(4);
    expect(new Set(sent).size).toBe(6); // every event exactly once
  });

  it('delivers in the background and wakes up on poke()', async () => {
    const producer = { publish: vi.fn(() => Promise.resolve([])) };
    const relay = new OutboxRelay(db, producer, logger, { pollMs: 60_000 });
    relay.start();
    await vi.waitFor(() => expect(producer.publish).toHaveBeenCalledTimes(6));

    await store.reserve(randomUUID(), [{ sku: 'A', quantity: 1 }], { correlationId: 'late' });
    relay.poke();
    await vi.waitFor(() => expect(producer.publish).toHaveBeenCalledTimes(8));
    await relay.stop();
  });
});
