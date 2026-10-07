import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { EventTypes, Topics, createEvent } from '@orderflow/contracts';
import type { HandlerContext } from '@orderflow/kafka-utils';
import { createFeedHandlers, createStateHandlers } from './handlers.js';
import { Hub, type Frame } from './hub.js';

const ctx = {} as HandlerContext;
const alice = { sub: 'alice', scopes: new Set(['orders:read', 'inventory:read']) };

describe('gateway handlers', () => {
  it('builds the stock table from the changelog', async () => {
    const hub = new Hub();
    const handle = createStateHandlers(hub)[Topics.StockLevels]!;
    for (const [version, available] of [
      [1, 9],
      [2, 8],
    ] as const) {
      await handle(
        createEvent(
          EventTypes.StockLevelChanged,
          {
            sku: 'A',
            name: 'Alpha',
            available,
            previousAvailable: available + 1,
            delta: -1,
            reason: 'reserved',
            version,
            lowStockThreshold: 2,
          },
          { correlationId: 'c' },
        ),
        ctx,
      );
    }
    expect(hub.snapshot(alice).stock).toEqual([
      expect.objectContaining({ sku: 'A', available: 8 }),
    ]);
  });

  it('publishes order and alert events to the feed, and every feed topic has a handler', async () => {
    const hub = new Hub();
    const frames: Frame[] = [];
    hub.connect(alice, (f) => frames.push(f));
    const handlers = createFeedHandlers(hub);

    expect(Object.keys(handlers).sort()).toEqual(
      [
        Topics.OrdersCreated,
        Topics.PaymentsCompleted,
        Topics.PaymentsFailed,
        Topics.InventoryReserved,
        Topics.InventoryRejected,
        Topics.InventoryReleased,
        Topics.StockLow,
      ].sort(),
    );

    await handlers[Topics.PaymentsFailed]!(
      createEvent(
        EventTypes.PaymentFailed,
        { orderId: randomUUID(), reason: 'declined' },
        { correlationId: 'c', actor: { sub: 'alice', clientId: 'web' } },
      ),
      ctx,
    );
    await handlers[Topics.StockLow]!(
      createEvent(
        EventTypes.StockLow,
        { sku: 'A', name: 'Alpha', available: 1, threshold: 2 },
        { correlationId: 'c' },
      ),
      ctx,
    );
    expect(frames.slice(1).map((f) => (f.data as { type: string }).type)).toEqual([
      'payment.failed',
      'stock.low',
    ]);
  });
});
