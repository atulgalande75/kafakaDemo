import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { EventTypes, createEvent } from '@orderflow/contracts';
import { toFeedEntry, toStockUpdate } from './feed.js';

const actor = { sub: 'user-1', clientId: 'web' };
const orderId = randomUUID();
const opts = { correlationId: 'corr-1', actor };

describe('toFeedEntry', () => {
  it('describes a new order and records who owns it', () => {
    const entry = toFeedEntry(
      createEvent(
        EventTypes.OrderCreated,
        {
          orderId,
          customerId: 'alice',
          items: [{ sku: 'SKU-MOUSE', quantity: 2, unitPrice: 10 }],
          totalAmount: 20,
          currency: 'USD',
        },
        opts,
      ),
    );
    expect(entry).toMatchObject({
      kind: 'order',
      type: 'order.created',
      orderId,
      ownerSub: 'user-1',
      correlationId: 'corr-1',
    });
    expect(entry?.summary).toContain('2× SKU-MOUSE');
    expect(entry?.summary).toContain('20.00 USD');
  });

  it.each([
    [
      EventTypes.PaymentFailed,
      { orderId, reason: 'card declined' },
      'payment failed - card declined',
    ],
    [
      EventTypes.InventoryRejected,
      { orderId, reason: 'Insufficient stock for SKU-GPU', unavailable: [] },
      'stock rejected - Insufficient stock for SKU-GPU',
    ],
    [
      EventTypes.InventoryReleased,
      {
        orderId,
        reservationId: randomUUID(),
        items: [{ sku: 'A', quantity: 1 }],
        reason: 'payment failed',
      },
      'reservation released (1× A)',
    ],
  ] as const)('summarises %s', (type, data, text) => {
    const entry = toFeedEntry(createEvent(type, data as never, opts));
    expect(entry?.kind).toBe('order');
    expect(entry?.summary).toContain(text);
    expect(entry?.ownerSub).toBe('user-1');
  });

  it('turns stock.low into an alert that is not tied to an order', () => {
    const entry = toFeedEntry(
      createEvent(
        EventTypes.StockLow,
        { sku: 'SKU-WEBCAM', name: 'HD webcam', available: 4, threshold: 5 },
        { correlationId: 'c' },
      ),
    );
    expect(entry).toMatchObject({ kind: 'stock-alert', sku: 'SKU-WEBCAM' });
    expect(entry?.ownerSub).toBeUndefined();
    expect(entry?.summary).toContain('4 left');
  });

  it('has no owner when the event has no actor', () => {
    const entry = toFeedEntry(
      createEvent(EventTypes.PaymentFailed, { orderId, reason: 'x' }, { correlationId: 'c' }),
    );
    expect(entry?.ownerSub).toBeUndefined();
  });

  it('ignores stock level changes (they are state, not feed)', () => {
    const event = createEvent(
      EventTypes.StockLevelChanged,
      {
        sku: 'A',
        name: 'Alpha',
        available: 1,
        previousAvailable: 2,
        delta: -1,
        reason: 'reserved',
        version: 1,
        lowStockThreshold: 1,
      },
      { correlationId: 'c' },
    );
    expect(toFeedEntry(event)).toBeUndefined();
    expect(toStockUpdate(event)).toMatchObject({ sku: 'A', available: 1, version: 1, delta: -1 });
  });
});
