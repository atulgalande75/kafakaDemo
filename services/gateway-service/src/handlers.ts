import { Topics, type AnyEvent } from '@orderflow/contracts';
import type { EventHandler, EventHandlers } from '@orderflow/kafka-utils';
import { toFeedEntry, toStockUpdate } from './feed.js';
import type { Hub } from './hub.js';

/** Topics replayed from the beginning: the compacted stock changelog rebuilds the stock table. */
export function createStateHandlers(hub: Hub): EventHandlers {
  return {
    [Topics.StockLevels]: (event) => {
      hub.applyStock(toStockUpdate(event));
      return Promise.resolve();
    },
  };
}

/** Topics read from "now" on: events for the live feed. */
export function createFeedHandlers(hub: Hub): EventHandlers {
  const feed: EventHandler<AnyEvent> = (event) => {
    const entry = toFeedEntry(event);
    if (entry) hub.publishFeed(entry);
    return Promise.resolve();
  };
  return {
    [Topics.OrdersCreated]: feed,
    [Topics.PaymentsCompleted]: feed,
    [Topics.PaymentsFailed]: feed,
    [Topics.InventoryReserved]: feed,
    [Topics.InventoryRejected]: feed,
    [Topics.InventoryReleased]: feed,
    [Topics.StockLow]: feed,
  };
}
