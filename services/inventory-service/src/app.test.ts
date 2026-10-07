import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { pino } from 'pino';
import { createTestIssuer, type TestIssuer } from '@orderflow/auth/testing';
import type { FastifyInstance } from 'fastify';
import { buildApp } from './app.js';
import { InventoryStore } from './inventory.js';
import { createTestDb } from './test-db.js';

let db: Awaited<ReturnType<typeof createTestDb>>;
let store: InventoryStore;
let issuer: TestIssuer;
let app: FastifyInstance;
const relay = { poke: vi.fn() };

beforeAll(async () => {
  db = await createTestDb();
  store = new InventoryStore(db);
  issuer = await createTestIssuer();
  app = buildApp({
    inventory: store,
    relay,
    logger: pino({ level: 'silent' }),
    verifier: issuer.verifier,
  });
});
afterAll(async () => {
  await app.close();
  await db.close();
});
beforeEach(async () => {
  await db.reset();
  await store.seed([
    { sku: 'A', name: 'Alpha', available: 5, lowStockThreshold: 2 },
    { sku: 'B', name: 'Beta', available: 1, lowStockThreshold: 2 },
  ]);
  relay.poke.mockClear();
});

const as = async (scope: string) => ({
  authorization: `Bearer ${await issuer.sign({ scope, sub: 'manager-1' })}`,
});
const adjust = async (sku: string, body: unknown, scope = 'inventory:write') =>
  app.inject({
    method: 'POST',
    url: `/inventory/${sku}/adjust`,
    headers: await as(scope),
    payload: body as object,
  });

describe('authentication and scopes', () => {
  it('serves /health without a token', async () => {
    expect((await app.inject('/health')).statusCode).toBe(200);
  });

  it('requires a token to read stock', async () => {
    const res = await app.inject('/inventory');
    expect(res.statusCode).toBe(401);
    expect(res.headers['www-authenticate']).toContain('Bearer');
  });

  it('lets inventory:read view but not change stock', async () => {
    const list = await app.inject({ url: '/inventory', headers: await as('inventory:read') });
    expect(list.statusCode).toBe(200);
    const res = await adjust('A', { delta: 1, reason: 'restock' }, 'inventory:read');
    expect(res.statusCode).toBe(403);
    expect(res.json()).toMatchObject({ error: 'insufficient_scope' });
  });

  it('does not accept order scopes for stock', async () => {
    const res = await app.inject({ url: '/inventory', headers: await as('orders:read') });
    expect(res.statusCode).toBe(403);
  });
});

describe('reading stock', () => {
  it('lists every SKU with its low-stock flag', async () => {
    const res = await app.inject({ url: '/inventory', headers: await as('inventory:read') });
    expect(res.json()).toEqual([
      expect.objectContaining({ sku: 'A', available: 5, low: false, version: 0 }),
      expect.objectContaining({ sku: 'B', available: 1, low: true }),
    ]);
  });

  it('returns one SKU or 404', async () => {
    const headers = await as('inventory:read');
    expect((await app.inject({ url: '/inventory/A', headers })).json()).toMatchObject({ sku: 'A' });
    expect((await app.inject({ url: '/inventory/NOPE', headers })).statusCode).toBe(404);
  });
});

describe('POST /inventory/:sku/adjust', () => {
  it('restocks, queues events attributed to the caller, and wakes the relay', async () => {
    const res = await adjust('B', { delta: 10, reason: 'restock', note: 'delivery' });
    expect(res.statusCode).toBe(200);
    expect(res.json()).toMatchObject({ sku: 'B', available: 11, version: 1, low: false });
    expect(relay.poke).toHaveBeenCalled();

    const { rows } = await db.query<{ event: { actor: unknown; data: { note: string } } }>(
      `SELECT event FROM outbox WHERE topic = 'inventory.stock-levels'`,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]!.event.actor).toEqual({ sub: 'manager-1', clientId: 'test-client' });
    expect(rows[0]!.event.data.note).toBe('delivery');
  });

  it('409s when stock would go negative', async () => {
    const res = await adjust('B', { delta: -5, reason: 'shrinkage' });
    expect(res.statusCode).toBe(409);
    expect(res.json()).toMatchObject({ error: 'insufficient_stock', available: 1 });
  });

  it('404s for an unknown SKU', async () => {
    expect((await adjust('NOPE', { delta: 1, reason: 'restock' })).statusCode).toBe(404);
  });

  it.each([
    [{ delta: 0, reason: 'correction' }],
    [{ delta: 1.5, reason: 'correction' }],
    [{ delta: -1, reason: 'restock' }],
    [{ delta: 1, reason: 'shrinkage' }],
    [{ delta: 1, reason: 'theft' }],
    [{}],
  ])('400s for an invalid body %j', async (body) => {
    const res = await adjust('A', body);
    expect(res.statusCode).toBe(400);
    expect((await store.get('A'))?.available).toBe(5);
  });
});
