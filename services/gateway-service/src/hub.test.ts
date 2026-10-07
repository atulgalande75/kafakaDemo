import { describe, expect, it } from 'vitest';
import { Hub, type Frame, type NewFeedEntry, type Principal, type StockUpdate } from './hub.js';

const principal = (sub: string, ...scopes: string[]): Principal => ({
  sub,
  scopes: new Set(scopes),
});
const alice = principal('alice', 'stream:read', 'orders:read', 'inventory:read');
const bob = principal('bob', 'stream:read', 'orders:read');
const admin = principal('root', 'stream:read', 'orders:read', 'admin');
const stockOnly = principal('wh', 'stream:read', 'inventory:read');

const stock = (over: Partial<StockUpdate> = {}): StockUpdate => ({
  sku: 'A',
  name: 'Alpha',
  available: 10,
  lowStockThreshold: 3,
  version: 1,
  occurredAt: '2026-01-01T00:00:00.000Z',
  delta: -1,
  reason: 'reserved',
  ...over,
});

const order = (ownerSub: string | undefined, orderId = 'o-1'): NewFeedEntry => ({
  kind: 'order',
  type: 'order.created',
  occurredAt: '2026-01-01T00:00:00.000Z',
  correlationId: 'c',
  summary: `order ${orderId}`,
  orderId,
  data: {},
  ...(ownerSub && { ownerSub }),
});
const alert: NewFeedEntry = {
  kind: 'stock-alert',
  type: 'stock.low',
  occurredAt: '2026-01-01T00:00:00.000Z',
  correlationId: 'c',
  summary: 'low',
  sku: 'A',
  data: {},
};

function client(hub: Hub, p: Principal, lastEventId?: string) {
  const frames: Frame[] = [];
  const disconnect = hub.connect(p, (f) => frames.push(f), lastEventId);
  return { frames, disconnect, live: () => frames.slice(1) };
}

describe('stock state', () => {
  it('keeps the latest level per SKU and sends the whole table on connect', () => {
    const hub = new Hub();
    hub.applyStock(stock({ sku: 'B', name: 'Beta' }));
    hub.applyStock(stock({ available: 8, version: 2 }));
    const { frames } = client(hub, alice);
    expect(frames[0]).toMatchObject({ event: 'snapshot' });
    const snap = frames[0]!.data as { stock: Array<{ sku: string; available: number }> };
    expect(snap.stock.map((s) => [s.sku, s.available])).toEqual([
      ['A', 8],
      ['B', 10],
    ]);
  });

  it('ignores older versions, so out-of-order records cannot roll a level back', () => {
    const hub = new Hub();
    hub.applyStock(stock({ available: 5, version: 3 }));
    expect(hub.applyStock(stock({ available: 9, version: 2 }))).toBe(false);
    expect(hub.snapshot(alice).stock?.[0]?.available).toBe(5);
  });

  it('ignores a resync that changes nothing but accepts one that does', () => {
    const hub = new Hub();
    hub.applyStock(stock({ version: 4 }));
    const { live } = client(hub, alice);
    expect(hub.applyStock(stock({ version: 4, reason: 'snapshot', delta: 0 }))).toBe(false);
    expect(hub.applyStock(stock({ version: 4, available: 12 }))).toBe(true);
    expect(live()).toHaveLength(1);
  });

  it('flags low stock from the threshold', () => {
    const hub = new Hub();
    hub.applyStock(stock({ available: 3 }));
    expect(hub.snapshot(alice).stock?.[0]).toMatchObject({ low: true });
  });

  it('pushes stock updates only to clients that may see stock', () => {
    const hub = new Hub();
    const a = client(hub, alice);
    const b = client(hub, bob);
    hub.applyStock(stock());
    expect(a.live()).toEqual([expect.objectContaining({ event: 'stock' })]);
    expect(b.live()).toEqual([]);
    expect(hub.snapshot(bob).stock).toBeNull();
  });
});

describe('feed visibility', () => {
  it('shows an order only to its owner and to admins', () => {
    const hub = new Hub();
    const a = client(hub, alice);
    const b = client(hub, bob);
    const r = client(hub, admin);
    hub.publishFeed(order('alice'));
    expect(a.live()).toHaveLength(1);
    expect(b.live()).toHaveLength(0);
    expect(r.live()).toHaveLength(1);
  });

  it('hides orders without a known owner from everyone but admins', () => {
    const hub = new Hub();
    hub.publishFeed(order(undefined));
    expect(hub.snapshot(alice).feed).toHaveLength(0);
    expect(hub.snapshot(admin).feed).toHaveLength(1);
  });

  it('never shows orders to a token without orders:read, even for its own sub', () => {
    const hub = new Hub();
    hub.publishFeed(order('wh'));
    expect(hub.snapshot(stockOnly).feed).toHaveLength(0);
  });

  it('shows stock alerts to stock viewers only', () => {
    const hub = new Hub();
    hub.publishFeed(alert);
    expect(hub.snapshot(alice).feed).toHaveLength(1);
    expect(hub.snapshot(bob).feed).toHaveLength(0);
    expect(hub.snapshot(stockOnly).feed).toHaveLength(1);
  });

  it('does not leak the owner to clients', () => {
    const hub = new Hub();
    hub.publishFeed(order('alice'));
    expect(JSON.stringify(hub.snapshot(alice))).not.toContain('ownerSub');
  });
});

describe('reconnect and resume', () => {
  it('gives a new client the most recent entries it may see', () => {
    const hub = new Hub({ recentCount: 2 });
    for (let i = 1; i <= 4; i++) hub.publishFeed(order('alice', `o-${i}`));
    const snap = hub.snapshot(alice);
    expect(snap.resumed).toBe(false);
    expect(snap.feed.map((e) => e.orderId)).toEqual(['o-3', 'o-4']);
  });

  it('replays only what a reconnecting client missed', () => {
    const hub = new Hub({ epoch: 'e1' });
    hub.publishFeed(order('alice', 'o-1'));
    const first = client(hub, alice);
    const lastId = first.frames[0]!.id; // snapshot id = current position
    first.disconnect();

    hub.publishFeed(order('alice', 'o-2'));
    hub.publishFeed(order('bob', 'o-3'));
    hub.publishFeed(order('alice', 'o-4'));

    const snap = hub.snapshot(alice, lastId);
    expect(snap.resumed).toBe(true);
    expect(snap.feed.map((e) => e.orderId)).toEqual(['o-2', 'o-4']);
  });

  it('advances Last-Event-ID with every feed frame', () => {
    const hub = new Hub({ epoch: 'e1' });
    const c = client(hub, alice);
    hub.publishFeed(order('alice'));
    expect(c.frames.map((f) => f.id)).toEqual(['e1:0', 'e1:1']);
  });

  it('falls back to a fresh snapshot for an id from another run', () => {
    const hub = new Hub({ epoch: 'new' });
    hub.publishFeed(order('alice'));
    const snap = hub.snapshot(alice, 'old:5');
    expect(snap.resumed).toBe(false);
    expect(snap.feed).toHaveLength(1);
  });

  it('falls back to a fresh snapshot when the missed entries were already evicted', () => {
    const hub = new Hub({ epoch: 'e', bufferSize: 2, recentCount: 10 });
    for (let i = 1; i <= 5; i++) hub.publishFeed(order('alice', `o-${i}`));
    expect(hub.snapshot(alice, 'e:1').resumed).toBe(false); // 2..3 are gone
    const ok = hub.snapshot(alice, 'e:3'); // 4 and 5 are still buffered
    expect(ok.resumed).toBe(true);
    expect(ok.feed.map((e) => e.orderId)).toEqual(['o-4', 'o-5']);
  });

  it('rejects ids that make no sense', () => {
    const hub = new Hub({ epoch: 'e' });
    hub.publishFeed(order('alice'));
    for (const bad of ['garbage', 'e:99', 'e:-1', '']) {
      expect(hub.snapshot(alice, bad).resumed).toBe(false);
    }
  });

  it('keeps the buffer bounded', () => {
    const hub = new Hub({ bufferSize: 3, recentCount: 100 });
    for (let i = 0; i < 10; i++) hub.publishFeed(order('alice', `o-${i}`));
    expect(hub.snapshot(alice).feed.map((e) => e.orderId)).toEqual(['o-7', 'o-8', 'o-9']);
  });
});

describe('connections', () => {
  it('stops sending after disconnect', () => {
    const hub = new Hub();
    const c = client(hub, alice);
    expect(hub.clients).toBe(1);
    c.disconnect();
    hub.publishFeed(order('alice'));
    expect(hub.clients).toBe(0);
    expect(c.live()).toEqual([]);
  });

  it('survives a client whose send throws', () => {
    const hub = new Hub();
    let first = true;
    hub.connect(alice, () => {
      if (!first) throw new Error('socket closed');
      first = false;
    });
    const healthy = client(hub, alice);
    hub.publishFeed(order('alice'));
    expect(healthy.live()).toHaveLength(1);
    expect(hub.clients).toBe(1);
  });
});
