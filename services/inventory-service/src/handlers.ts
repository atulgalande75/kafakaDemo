import {
  EventTypes,
  Topics,
  createEvent,
  deriveEventId,
  type OrderCreatedEvent,
} from '@orderflow/contracts';
import type { EventHandlers, EventProducer, HandlerContext } from '@orderflow/kafka-utils';
import type { Inventory } from './inventory.js';

export function createInventoryHandlers(
  producer: Pick<EventProducer, 'publish'>,
  inventory: Inventory,
): EventHandlers {
  return {
    [Topics.OrdersCreated]: async (event: OrderCreatedEvent, { log }: HandlerContext) => {
      const { orderId, items } = event.data;
      const result = inventory.reserve(orderId, items);
      const options = {
        correlationId: event.correlationId,
        eventId: deriveEventId(event.eventId, 'inventory'),
      };

      if (result.ok) {
        await producer.publish(
          Topics.InventoryReserved,
          createEvent(
            EventTypes.InventoryReserved,
            { orderId, reservationId: result.reservationId, items: result.items },
            options,
          ),
          { key: orderId },
        );
        log.info(
          {
            orderId,
            items: result.items,
            alreadyReserved: result.alreadyReserved,
            remaining: Object.fromEntries(
              result.items.map((i) => [i.sku, inventory.available(i.sku)]),
            ),
          },
          'inventory reserved',
        );
      } else {
        await producer.publish(
          Topics.InventoryRejected,
          createEvent(
            EventTypes.InventoryRejected,
            { orderId, reason: result.reason, unavailable: result.unavailable },
            options,
          ),
          { key: orderId },
        );
        log.info({ orderId, reason: result.reason }, 'inventory rejected');
      }
    },
  };
}
