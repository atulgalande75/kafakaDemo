import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { EventTypes, createEvent } from '@orderflow/contracts';
import { applyOutcome, orderFromEvent, type Order } from './order.js';

const opts = { correlationId: 'corr-1' };

function newOrder(): Order {
  return orderFromEvent(
    createEvent(
      EventTypes.OrderCreated,
      {
        orderId: randomUUID(),
        customerId: 'c-1',
        items: [{ sku: 'SKU-MOUSE', quantity: 1, unitPrice: 25 }],
        totalAmount: 25,
        currency: 'USD',
      },
      opts,
    ),
  );
}

const paid = (orderId: string) =>
  createEvent(
    EventTypes.PaymentCompleted,
    { orderId, paymentId: randomUUID(), amount: 25, currency: 'USD' },
    opts,
  );
const declined = (orderId: string) =>
  createEvent(EventTypes.PaymentFailed, { orderId, reason: 'card declined' }, opts);
const reserved = (orderId: string) =>
  createEvent(
    EventTypes.InventoryReserved,
    { orderId, reservationId: randomUUID(), items: [{ sku: 'SKU-MOUSE', quantity: 1 }] },
    opts,
  );
const rejected = (orderId: string) =>
  createEvent(
    EventTypes.InventoryRejected,
    { orderId, reason: 'out of stock', unavailable: [] },
    opts,
  );

describe('order saga', () => {
  it('starts PENDING', () => {
    const order = newOrder();
    expect(order.status).toBe('PENDING');
    expect(order.history).toHaveLength(1);
  });

  it('stays PENDING until both sides succeed, then CONFIRMS (in any order)', () => {
    const a = newOrder();
    const afterPayment = applyOutcome(a, paid(a.id));
    expect(afterPayment.status).toBe('PENDING');
    expect(applyOutcome(afterPayment, reserved(a.id)).status).toBe('CONFIRMED');

    const b = newOrder();
    expect(applyOutcome(applyOutcome(b, reserved(b.id)), paid(b.id)).status).toBe('CONFIRMED');
  });

  it('CANCELS as soon as either side fails', () => {
    const a = newOrder();
    expect(applyOutcome(a, declined(a.id)).status).toBe('CANCELLED');
    const b = newOrder();
    expect(applyOutcome(b, rejected(b.id)).status).toBe('CANCELLED');
  });

  it('keeps CANCELLED final even if the other side later succeeds', () => {
    const a = newOrder();
    const cancelled = applyOutcome(a, rejected(a.id));
    const after = applyOutcome(cancelled, paid(a.id));
    expect(after.status).toBe('CANCELLED');
    expect(after.payment.status).toBe('COMPLETED');
  });

  it('ignores an event it has already applied', () => {
    const a = newOrder();
    const event = paid(a.id);
    const once = applyOutcome(a, event);
    expect(applyOutcome(once, event)).toBe(once);
  });
});
