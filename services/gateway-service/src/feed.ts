import type { AnyEvent } from '@orderflow/contracts';
import type { NewFeedEntry, StockUpdate } from './hub.js';

const short = (id: string) => id.slice(0, 8);
const money = (n: number, currency: string) => `${n.toFixed(2)} ${currency}`;
const lines = (items: Array<{ sku: string; quantity: number }>) =>
  items.map((i) => `${i.quantity}× ${i.sku}`).join(', ');

/** Turns a pipeline event into a feed entry, or undefined for events that aren't shown. */
export function toFeedEntry(event: AnyEvent): NewFeedEntry | undefined {
  const base = {
    type: event.type,
    occurredAt: event.occurredAt,
    correlationId: event.correlationId,
    ...(event.actor && { ownerSub: event.actor.sub }),
  };

  switch (event.type) {
    case 'order.created': {
      const { orderId, customerId, items, totalAmount, currency } = event.data;
      return {
        ...base,
        kind: 'order',
        orderId,
        data: event.data,
        summary: `Order ${short(orderId)} placed by ${customerId}: ${lines(items)} (${money(totalAmount, currency)})`,
      };
    }
    case 'payment.completed': {
      const { orderId, amount, currency } = event.data;
      return {
        ...base,
        kind: 'order',
        orderId,
        data: event.data,
        summary: `Order ${short(orderId)}: payment of ${money(amount, currency)} completed`,
      };
    }
    case 'payment.failed':
      return {
        ...base,
        kind: 'order',
        orderId: event.data.orderId,
        data: event.data,
        summary: `Order ${short(event.data.orderId)}: payment failed - ${event.data.reason}`,
      };
    case 'inventory.reserved':
      return {
        ...base,
        kind: 'order',
        orderId: event.data.orderId,
        data: event.data,
        summary: `Order ${short(event.data.orderId)}: stock reserved (${lines(event.data.items)})`,
      };
    case 'inventory.rejected':
      return {
        ...base,
        kind: 'order',
        orderId: event.data.orderId,
        data: event.data,
        summary: `Order ${short(event.data.orderId)}: stock rejected - ${event.data.reason}`,
      };
    case 'inventory.released':
      return {
        ...base,
        kind: 'order',
        orderId: event.data.orderId,
        data: event.data,
        summary: `Order ${short(event.data.orderId)}: reservation released (${lines(event.data.items)}) - ${event.data.reason}`,
      };
    case 'stock.low': {
      const { sku, name, available, threshold } = event.data;
      return {
        ...base,
        kind: 'stock-alert',
        sku,
        data: event.data,
        summary: `${name} (${sku}) is low: ${available} left (threshold ${threshold})`,
      };
    }
    default:
      return undefined;
  }
}

export function toStockUpdate(
  event: Extract<AnyEvent, { type: 'stock.level.changed' }>,
): StockUpdate {
  const d = event.data;
  return {
    sku: d.sku,
    name: d.name,
    available: d.available,
    lowStockThreshold: d.lowStockThreshold,
    version: d.version,
    occurredAt: event.occurredAt,
    delta: d.delta,
    reason: d.reason,
    ...(d.orderId && { orderId: d.orderId }),
  };
}
