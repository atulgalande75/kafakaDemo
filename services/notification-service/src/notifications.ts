import type { NotificationChannel } from '@orderflow/feature-flags';
import type {
  InventoryRejectedEvent,
  InventoryReservedEvent,
  PaymentCompletedEvent,
  PaymentFailedEvent,
} from '@orderflow/contracts';

export type OutcomeEvent =
  PaymentCompletedEvent | PaymentFailedEvent | InventoryReservedEvent | InventoryRejectedEvent;

export interface Notification {
  orderId: string;
  channel: NotificationChannel;
  subject: string;
  body: string;
}

const short = (id: string) => id.slice(0, 8);

/** Renders the customer notification for a payment or inventory outcome. */
export function renderNotification(
  event: OutcomeEvent,
  channel: NotificationChannel = 'email',
): Notification {
  const { orderId } = event.data;
  const base = { orderId, channel };
  switch (event.type) {
    case 'payment.completed':
      return {
        ...base,
        subject: `Payment received for order ${short(orderId)}`,
        body: `We received your payment of ${event.data.amount.toFixed(2)} ${event.data.currency}.`,
      };
    case 'payment.failed':
      return {
        ...base,
        subject: `Payment failed for order ${short(orderId)}`,
        body: `Your payment could not be processed: ${event.data.reason}.`,
      };
    case 'inventory.reserved':
      return {
        ...base,
        subject: `Items reserved for order ${short(orderId)}`,
        body: `Good news - ${event.data.items.map((i) => `${i.quantity}x ${i.sku}`).join(', ')} set aside for you.`,
      };
    case 'inventory.rejected':
      return {
        ...base,
        subject: `Items unavailable for order ${short(orderId)}`,
        body: `Sorry, we could not reserve your items: ${event.data.reason}.`,
      };
  }
}
