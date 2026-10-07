import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  InsufficientStockError,
  InventoryStore,
  UnknownSkuError,
  type Cause,
} from './inventory.js';
import { createTestDb } from './test-db.js';

let db: Awaited<ReturnType<typeof createTestDb>>;
let store: InventoryStore;

const CATALOG = [
  { sku: 'A', name: 'Alpha', available: 5, lowStockThreshold: 2 },
  { sku: 'B', name: 'Beta', available: 2, lowStockThreshold: 1 },
  { sku: 'Z', name: 'Zero', available: 0, lowStockThreshold: 1 },
];

const cause = (): Cause => ({ eventId: randomUUID(), correlationId: 'corr-1' });

interface OutboxRow {
  topic: string;
  key: string;
  event: { eventId: string; type: string; data: Record<string, unknown> };
}
const outbox = async (topic?: string) => {
  const { rows } = await db.query<OutboxRow>('SELECT topic, key, event FROM outbox ORDER BY id');
  return topic ? rows.filter((r) => r.topic === topic) : rows;
};
const levels = async () => (await outbox('inventory.stock-levels')).map((r) => r.event.data);

beforeAll(async () => {
  db = await createTestDb();
  store = new InventoryStore(db);
});
afterAll(() => db.close());
beforeEach(async () => {
  await db.reset();
  await store.seed(CATALOG);
});

describe('seed', () => {
  it('inserts missing SKUs and never overwrites existing stock', async () => {
    await store.adjust('A', 3, 'restock', cause());
    expect(await store.seed(CATALOG)).toBe(0);
    expect((await store.get('A'))?.available).toBe(8);
  });
});

describe('reserve', () => {
  it('takes all lines, aggregating repeated SKUs', async () => {
    const orderId = randomUUID();
    const result = await store.reserve(
      orderId,
      [
        { sku: 'A', quantity: 2 },
        { sku: 'B', quantity: 1 },
        { sku: 'A', quantity: 1 },
      ],
      cause(),
    );
    expect(result).toMatchObject({
      outcome: 'reserved',
      duplicate: false,
      items: [
        { sku: 'A', quantity: 3 },
        { sku: 'B', quantity: 1 },
      ],
    });
    expect((await store.list()).map((s) => [s.sku, s.available])).toEqual([
      ['A', 2],
      ['B', 1],
      ['Z', 0],
    ]);
  });

  it('rejects the whole order when any line is short, leaving stock unchanged', async () => {
    const result = await store.reserve(
      randomUUID(),
      [
        { sku: 'A', quantity: 1 },
        { sku: 'Z', quantity: 1 },
      ],
      cause(),
    );
    expect(result).toEqual({
      outcome: 'rejected',
      reason: 'Insufficient stock for Z',
      unavailable: [{ sku: 'Z', requested: 1, available: 0 }],
      duplicate: false,
    });
    expect((await store.get('A'))?.available).toBe(5);
    expect(await levels()).toEqual([]);
  });

  it('rejects unknown SKUs', async () => {
    const result = await store.reserve(randomUUID(), [{ sku: 'NOPE', quantity: 1 }], cause());
    expect(result).toMatchObject({ outcome: 'rejected', reason: 'Unknown SKU(s): NOPE' });
  });

  it('is idempotent: handling the same order twice reserves once and emits once', async () => {
    const orderId = randomUUID();
    const c = cause();
    const first = await store.reserve(orderId, [{ sku: 'A', quantity: 2 }], c);
    const second = await store.reserve(orderId, [{ sku: 'A', quantity: 2 }], c);
    expect(second).toMatchObject({ outcome: 'reserved', duplicate: true });
    expect(
      first.outcome === 'reserved' && second.outcome === 'reserved' && second.reservationId,
    ).toBe(first.outcome === 'reserved' && first.reservationId);
    expect((await store.get('A'))?.available).toBe(3);
    expect(await outbox('inventory.reserved')).toHaveLength(1);
    expect(await levels()).toHaveLength(1);
  });

  it('remembers a rejection even if stock arrives before the redelivery', async () => {
    const orderId = randomUUID();
    await store.reserve(orderId, [{ sku: 'Z', quantity: 1 }], cause());
    await store.adjust('Z', 10, 'restock', cause());
    const again = await store.reserve(orderId, [{ sku: 'Z', quantity: 1 }], cause());
    expect(again).toMatchObject({ outcome: 'rejected', duplicate: true });
    expect((await store.get('Z'))?.available).toBe(10);
  });

  it('writes the events in the same transaction as the stock change', async () => {
    const orderId = randomUUID();
    const c = { ...cause(), actor: { sub: 'user-1', clientId: 'web' } };
    await store.reserve(orderId, [{ sku: 'A', quantity: 4 }], c);

    const rows = await outbox();
    expect(rows.map((r) => [r.topic, r.key])).toEqual([
      ['inventory.stock-levels', 'A'],
      ['inventory.stock-low', 'A'], // 5 -> 1 crosses the threshold of 2
      ['inventory.reserved', orderId],
    ]);
    expect(rows[0]!.event.data).toMatchObject({
      sku: 'A',
      available: 1,
      previousAvailable: 5,
      delta: -4,
      reason: 'reserved',
      version: 1,
      orderId,
    });
    expect(rows[2]!.event).toMatchObject({ correlationId: 'corr-1', actor: c.actor });
  });

  it('derives event ids from the causing event, so a retry after rollback emits the same ids', async () => {
    const c = cause();
    const orderId = randomUUID();
    await store.reserve(orderId, [{ sku: 'A', quantity: 1 }], c);
    const ids = (await outbox()).map((r) => r.event.eventId);

    await db.reset();
    await store.seed(CATALOG);
    await store.reserve(orderId, [{ sku: 'A', quantity: 1 }], c);
    expect((await outbox()).map((r) => r.event.eventId)).toEqual(ids);
  });

  it('emits stock.low only when crossing the threshold, not on every change below it', async () => {
    await store.reserve(randomUUID(), [{ sku: 'A', quantity: 3 }], cause()); // 5 -> 2: crosses
    await store.reserve(randomUUID(), [{ sku: 'A', quantity: 1 }], cause()); // 2 -> 1: already low
    expect(await outbox('inventory.stock-low')).toHaveLength(1);
  });

  it('never oversells under concurrent orders', async () => {
    const results = await Promise.all(
      Array.from({ length: 4 }, () =>
        store.reserve(randomUUID(), [{ sku: 'B', quantity: 1 }], cause()),
      ),
    );
    expect(results.filter((r) => r.outcome === 'reserved')).toHaveLength(2);
    expect((await store.get('B'))?.available).toBe(0);
  });
});

describe('release', () => {
  it('gives the reservation back and emits inventory.released', async () => {
    const orderId = randomUUID();
    await store.reserve(orderId, [{ sku: 'A', quantity: 3 }], cause());
    const result = await store.release(orderId, 'payment failed: declined', cause());

    expect(result).toMatchObject({ outcome: 'released', items: [{ sku: 'A', quantity: 3 }] });
    expect((await store.get('A'))?.available).toBe(5);
    const released = await outbox('inventory.released');
    expect(released).toHaveLength(1);
    expect(released[0]!.event.data).toMatchObject({ orderId, reason: 'payment failed: declined' });
    expect((await levels()).map((l) => [l.reason, l.version])).toEqual([
      ['reserved', 1],
      ['released', 2],
    ]);
  });

  it('releases only once', async () => {
    const orderId = randomUUID();
    await store.reserve(orderId, [{ sku: 'A', quantity: 3 }], cause());
    await store.release(orderId, 'x', cause());
    expect(await store.release(orderId, 'x', cause())).toEqual({
      outcome: 'nothing-to-release',
      status: 'released',
    });
    expect((await store.get('A'))?.available).toBe(5);
  });

  it('ignores an order that was rejected (nothing was taken)', async () => {
    const orderId = randomUUID();
    await store.reserve(orderId, [{ sku: 'Z', quantity: 1 }], cause());
    expect(await store.release(orderId, 'x', cause())).toMatchObject({
      outcome: 'nothing-to-release',
      status: 'rejected',
    });
  });

  it('handles payments.failed arriving before orders.created: the late reservation is skipped', async () => {
    const orderId = randomUUID();
    expect(await store.release(orderId, 'x', cause())).toEqual({
      outcome: 'cancelled-in-advance',
    });
    expect(await store.reserve(orderId, [{ sku: 'A', quantity: 1 }], cause())).toEqual({
      outcome: 'skipped',
      status: 'cancelled',
    });
    expect((await store.get('A'))?.available).toBe(5);
    expect(await outbox()).toEqual([]);
  });

  it('does not reserve again after a release (orders.created redelivered)', async () => {
    const orderId = randomUUID();
    await store.reserve(orderId, [{ sku: 'A', quantity: 1 }], cause());
    await store.release(orderId, 'x', cause());
    expect(await store.reserve(orderId, [{ sku: 'A', quantity: 1 }], cause())).toEqual({
      outcome: 'skipped',
      status: 'released',
    });
    expect((await store.get('A'))?.available).toBe(5);
  });
});

describe('adjust', () => {
  it('restocks and records why', async () => {
    const item = await store.adjust('Z', 25, 'restock', cause(), 'supplier delivery');
    expect(item).toMatchObject({ sku: 'Z', available: 25, version: 1, low: false });
    expect(await levels()).toEqual([
      expect.objectContaining({
        reason: 'restock',
        delta: 25,
        previousAvailable: 0,
        note: 'supplier delivery',
      }),
    ]);
  });

  it('refuses to take stock below zero', async () => {
    await expect(store.adjust('B', -3, 'shrinkage', cause())).rejects.toBeInstanceOf(
      InsufficientStockError,
    );
    expect((await store.get('B'))?.available).toBe(2);
    expect(await outbox()).toEqual([]);
  });

  it('rejects unknown SKUs', async () => {
    await expect(store.adjust('NOPE', 1, 'restock', cause())).rejects.toBeInstanceOf(
      UnknownSkuError,
    );
  });
});

describe('publishSnapshot', () => {
  it('enqueues the current level of every SKU without changing versions', async () => {
    await store.adjust('A', 1, 'restock', cause());
    await db.query('DELETE FROM outbox');
    expect(await store.publishSnapshot()).toBe(3);
    const snapshot = await levels();
    expect(snapshot.map((l) => [l.sku, l.available, l.reason, l.delta])).toEqual([
      ['A', 6, 'snapshot', 0],
      ['B', 2, 'snapshot', 0],
      ['Z', 0, 'snapshot', 0],
    ]);
    expect(snapshot[0]!.version).toBe(1);
  });
});
