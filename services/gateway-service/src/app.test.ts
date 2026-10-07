import type { AddressInfo } from 'node:net';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import { pino } from 'pino';
import { LocalFeatureFlags } from '@orderflow/feature-flags';
import { createTestIssuer, type TestIssuer } from '@orderflow/auth/testing';
import Fastify, { type FastifyInstance } from 'fastify';
import { buildApp } from './app.js';
import { Hub, type FeedEntry, type NewFeedEntry, type SnapshotPayload } from './hub.js';

let issuer: TestIssuer;
let hub: Hub;
let flags: LocalFeatureFlags;
let app: FastifyInstance;
let baseUrl: string;
const controllers: AbortController[] = [];

beforeAll(async () => {
  issuer = await createTestIssuer();
});

async function start(options: { heartbeatMs?: number } = {}) {
  hub = new Hub({ epoch: 'test' });
  flags = new LocalFeatureFlags({ logger: pino({ level: 'silent' }), env: {} });
  app = buildApp({
    hub,
    flags,
    logger: pino({ level: 'silent' }),
    verifier: issuer.verifier,
    allowedOrigins: ['http://localhost:5173'],
    ...options,
  });
  await app.listen({ port: 0, host: '127.0.0.1' });
  baseUrl = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
}

afterEach(async () => {
  controllers.splice(0).forEach((c) => c.abort());
  await app.close();
});

const bearer = async (sub: string, scope: string) => ({
  authorization: `Bearer ${await issuer.sign({ sub, scope })}`,
});

/** Opens /stream and returns a reader that yields complete SSE messages. */
async function openStream(headers: Record<string, string>) {
  const controller = new AbortController();
  controllers.push(controller);
  const res = await fetch(`${baseUrl}/stream`, { headers, signal: controller.signal });
  const reader = res.body!.pipeThrough(new TextDecoderStream()).getReader();
  let pending = '';
  const next = async (): Promise<string> => {
    for (;;) {
      const end = pending.indexOf('\n\n');
      if (end >= 0) {
        const message = pending.slice(0, end);
        pending = pending.slice(end + 2);
        return message;
      }
      const { value, done } = await reader.read();
      if (done) throw new Error('stream ended');
      pending += value;
    }
  };
  /** Next message that is not a heartbeat comment, parsed. */
  const nextFrame = async () => {
    for (;;) {
      const message = await next();
      if (message.startsWith(':')) continue;
      const field = (name: string) =>
        message
          .split('\n')
          .find((l) => l.startsWith(`${name}: `))
          ?.slice(name.length + 2);
      return {
        id: field('id'),
        event: field('event'),
        data: JSON.parse(field('data') ?? 'null') as Partial<SnapshotPayload> & Partial<FeedEntry>,
      };
    }
  };
  return { res, nextFrame, next };
}

const order = (ownerSub: string): NewFeedEntry => ({
  kind: 'order',
  type: 'order.created',
  occurredAt: '2026-01-01T00:00:00.000Z',
  correlationId: 'c',
  summary: 'new order',
  orderId: 'o-1',
  data: {},
  ownerSub,
});

describe('gateway HTTP', () => {
  it('serves /health without a token', async () => {
    await start();
    const res = await fetch(`${baseUrl}/health`);
    expect(await res.json()).toMatchObject({ status: 'ok', clients: 0 });
  });

  it('requires a token', async () => {
    await start();
    const res = await fetch(`${baseUrl}/stream`);
    expect(res.status).toBe(401);
    expect(res.headers.get('www-authenticate')).toContain('Bearer');
  });

  it('requires the stream:read scope', async () => {
    await start();
    const res = await fetch(`${baseUrl}/stream`, {
      headers: await bearer('alice', 'orders:read inventory:read'),
    });
    expect(res.status).toBe(403);
  });

  it('refuses a token that could see nothing', async () => {
    await start();
    const res = await fetch(`${baseUrl}/stream`, { headers: await bearer('alice', 'stream:read') });
    expect(res.status).toBe(403);
  });

  it('answers CORS preflights for the allowed origin only', async () => {
    await start();
    const ask = (origin: string) =>
      fetch(`${baseUrl}/stream`, {
        method: 'OPTIONS',
        headers: {
          origin,
          'access-control-request-method': 'GET',
          'access-control-request-headers': 'authorization,last-event-id',
        },
      });
    expect((await ask('http://localhost:5173')).headers.get('access-control-allow-origin')).toBe(
      'http://localhost:5173',
    );
    expect((await ask('http://evil.test')).headers.get('access-control-allow-origin')).toBeNull();
  });
});

describe('GET /stream', () => {
  it('sends SSE headers (including CORS) and a snapshot first', async () => {
    await start();
    hub.applyStock({
      sku: 'A',
      name: 'Alpha',
      available: 7,
      lowStockThreshold: 2,
      version: 1,
      occurredAt: '2026-01-01T00:00:00.000Z',
      delta: -1,
      reason: 'reserved',
    });
    const { res, nextFrame } = await openStream({
      ...(await bearer('alice', 'stream:read orders:read inventory:read')),
      origin: 'http://localhost:5173',
    });
    expect(res.headers.get('content-type')).toContain('text/event-stream');
    expect(res.headers.get('cache-control')).toContain('no-cache');
    expect(res.headers.get('access-control-allow-origin')).toBe('http://localhost:5173');

    const snapshot = await nextFrame();
    expect(snapshot).toMatchObject({ event: 'snapshot', id: 'test:0' });
    expect(snapshot.data.stock).toEqual([expect.objectContaining({ sku: 'A', available: 7 })]);
    expect(hub.clients).toBe(1);
  });

  it('streams live updates and only the ones this user may see', async () => {
    await start();
    const alice = await openStream(await bearer('alice', 'stream:read orders:read'));
    await alice.nextFrame(); // snapshot
    await alice.nextFrame(); // flags

    hub.publishFeed(order('bob')); // not alice's
    hub.publishFeed(order('alice'));
    const frame = await alice.nextFrame();
    expect(frame).toMatchObject({ event: 'feed', id: 'test:2' });
    expect(frame.data).toMatchObject({ seq: 2, orderId: 'o-1', summary: 'new order' });
    expect(JSON.stringify(frame.data)).not.toContain('ownerSub');
  });

  it('resumes from Last-Event-ID with only the missed entries', async () => {
    await start();
    hub.publishFeed(order('alice'));
    hub.publishFeed(order('alice'));
    hub.publishFeed(order('alice'));
    const { nextFrame } = await openStream({
      ...(await bearer('alice', 'stream:read orders:read')),
      'last-event-id': 'test:1',
    });
    const snapshot = await nextFrame();
    expect(snapshot.data.resumed).toBe(true);
    expect(snapshot.data.feed!.map((e: { seq: number }) => e.seq)).toEqual([2, 3]);
  });

  it('sends heartbeats on idle connections', async () => {
    await start({ heartbeatMs: 30 });
    const { next } = await openStream(await bearer('alice', 'stream:read orders:read'));
    await next(); // snapshot
    await next(); // flags
    expect(await next()).toBe('event: ping\ndata: {}');
  });

  it('ends open streams when the server shuts down, instead of waiting for clients', async () => {
    await start();
    const stream = await openStream(await bearer('alice', 'stream:read orders:read'));
    await stream.nextFrame(); // snapshot
    const started = Date.now();
    await app.close();
    const untilEnd = async () => {
      for (;;) await stream.next(); // the buffered flags frame first, then the end
    };
    await expect(untilEnd()).rejects.toThrow('stream ended'); // the client sees a clean end
    expect(Date.now() - started).toBeLessThan(2000);
    expect(hub.clients).toBe(0);
    app = Fastify(); // afterEach closes whatever `app` is
  });

  it('unregisters the client when it disconnects', async () => {
    await start();
    const controller = new AbortController();
    const res = await fetch(`${baseUrl}/stream`, {
      headers: await bearer('alice', 'stream:read orders:read'),
      signal: controller.signal,
    });
    await res.body!.getReader().read();
    expect(hub.clients).toBe(1);
    controller.abort();
    await expect.poll(() => hub.clients).toBe(0);
  });
});

describe('feature flags for the web app', () => {
  const alice = () => bearer('alice', 'stream:read orders:read');

  it('serves the defaults, per user, over REST', async () => {
    await start();
    const res = await fetch(`${baseUrl}/flags`, { headers: await alice() });
    expect(await res.json()).toEqual({
      liveUpdates: true,
      newInventoryDashboard: false,
      bulkAdjust: false,
      activityFeedSize: 50,
    });
  });

  it('requires the stream:read scope', async () => {
    await start();
    expect((await fetch(`${baseUrl}/flags`)).status).toBe(401);
    const res = await fetch(`${baseUrl}/flags`, { headers: await bearer('a', 'orders:read') });
    expect(res.status).toBe(403);
  });

  it('reflects configured flag values', async () => {
    await start();
    flags.set('new-inventory-dashboard', true);
    flags.set('activity-feed-size', 20);
    const res = await fetch(`${baseUrl}/flags`, { headers: await alice() });
    expect(await res.json()).toMatchObject({ newInventoryDashboard: true, activityFeedSize: 20 });
  });

  it('adds the flags to /snapshot for clients that poll', async () => {
    await start();
    flags.set('live-updates-enabled', false);
    const res = await fetch(`${baseUrl}/snapshot`, { headers: await alice() });
    expect(await res.json()).toMatchObject({
      resumed: false,
      flags: { liveUpdates: false },
    });
  });

  it('sends the flags right after the snapshot when a stream opens', async () => {
    await start();
    flags.set('bulk-adjust-enabled', true);
    const { nextFrame } = await openStream(await alice());
    expect((await nextFrame()).event).toBe('snapshot');
    const frame = await nextFrame();
    expect(frame.event).toBe('flags');
    expect(frame.data).toMatchObject({ bulkAdjust: true, liveUpdates: true });
  });

  it('pushes a flag change to open streams, once per actual change', async () => {
    await start();
    const { nextFrame } = await openStream(await alice());
    await nextFrame(); // snapshot
    await nextFrame(); // flags

    flags.set('payment-failure-rate', 0.5); // not a web flag: nothing to send
    flags.set('new-inventory-dashboard', true);
    const frame = await nextFrame();
    expect(frame).toMatchObject({ event: 'flags', data: { newInventoryDashboard: true } });

    flags.set('live-updates-enabled', false);
    expect(await nextFrame()).toMatchObject({ event: 'flags', data: { liveUpdates: false } });
  });

  it('evaluates flags for the signed-in user and stops listening when they leave', async () => {
    const seen: unknown[] = [];
    const listeners = new Set<unknown>();
    const fake = {
      provider: 'local' as const,
      get: (_key: string, context: unknown) => {
        seen.push(context);
        return Promise.resolve(true);
      },
      onChange: (listener: unknown) => {
        listeners.add(listener);
        return () => listeners.delete(listener);
      },
      close: () => Promise.resolve(),
    };
    hub = new Hub({ epoch: 'test' });
    app = buildApp({
      hub,
      flags: fake as never,
      logger: pino({ level: 'silent' }),
      verifier: issuer.verifier,
    });
    await app.listen({ port: 0, host: '127.0.0.1' });
    baseUrl = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;

    const token = await issuer.sign({
      sub: 'sub-42',
      scope: 'stream:read orders:read',
      preferred_username: 'alice',
    });
    const controller = new AbortController();
    const res = await fetch(`${baseUrl}/stream`, {
      headers: { authorization: `Bearer ${token}` },
      signal: controller.signal,
    });
    await res.body!.getReader().read();
    expect(seen).toContainEqual({ kind: 'user', sub: 'sub-42', username: 'alice' });
    expect(listeners.size).toBe(1);

    controller.abort();
    await expect.poll(() => listeners.size).toBe(0);
  });
});

describe('GET /snapshot', () => {
  it('returns the same state as plain JSON', async () => {
    await start();
    hub.publishFeed(order('alice'));
    const res = await fetch(`${baseUrl}/snapshot`, {
      headers: await bearer('alice', 'stream:read orders:read'),
    });
    expect(await res.json()).toMatchObject({ stock: null, resumed: false, feed: [{ seq: 1 }] });
  });
});
