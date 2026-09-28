import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { EventTypes, createEvent } from '@orderflow/contracts';
import { renderNotification } from './notifications.js';

const orderId = randomUUID();
const opts = { correlationId: 'c' };

describe('renderNotification', () => {
  it('renders one message per outcome type', () => {
    const events = [
      createEvent(
        EventTypes.PaymentCompleted,
        { orderId, paymentId: randomUUID(), amount: 12.5, currency: 'USD' },
        opts,
      ),
      createEvent(EventTypes.PaymentFailed, { orderId, reason: 'Card declined' }, opts),
      createEvent(
        EventTypes.InventoryReserved,
        { orderId, reservationId: randomUUID(), items: [{ sku: 'SKU-MOUSE', quantity: 2 }] },
        opts,
      ),
      createEvent(
        EventTypes.InventoryRejected,
        { orderId, reason: 'Insufficient stock for SKU-GPU', unavailable: [] },
        opts,
      ),
    ];
    expect(events.map((e) => renderNotification(e).body)).toEqual([
      'We received your payment of 12.50 USD.',
      'Your payment could not be processed: Card declined.',
      'Good news - 2x SKU-MOUSE set aside for you.',
      'Sorry, we could not reserve your items: Insufficient stock for SKU-GPU.',
    ]);
    expect(renderNotification(events[0]!).subject).toBe(
      `Payment received for order ${orderId.slice(0, 8)}`,
    );
  });

  it('uses the channel from the notification-channel flag', () => {
    const event = createEvent(EventTypes.PaymentFailed, { orderId, reason: 'x' }, opts);
    expect(renderNotification(event).channel).toBe('email');
    expect(renderNotification(event, 'sms').channel).toBe('sms');
  });
});
