import { Topics, type PaymentFailedEvent, type OrderCreatedEvent } from '@orderflow/contracts';
import type { EventHandlers, HandlerContext } from '@orderflow/kafka-utils';
import type { Cause, InventoryStore } from './inventory.js';

const causeOf = (event: {
  eventId: string;
  correlationId: string;
  actor?: Cause['actor'];
}): Cause => ({
  eventId: event.eventId,
  correlationId: event.correlationId,
  actor: event.actor,
});

/**
 * Reacts to orders and payment failures. The resulting events go to the outbox in the same
 * transaction as the stock change; `relay.poke()` just makes the relay send them right away.
 */
export function createInventoryHandlers(
  inventory: InventoryStore,
  relay: { poke(): void },
): EventHandlers {
  return {
    [Topics.OrdersCreated]: async (event: OrderCreatedEvent, { log }: HandlerContext) => {
      const { orderId, items } = event.data;
      const result = await inventory.reserve(orderId, items, causeOf(event));
      relay.poke();

      switch (result.outcome) {
        case 'reserved':
          log.info(
            { orderId, items: result.items, alreadyReserved: result.duplicate },
            'inventory reserved',
          );
          break;
        case 'rejected':
          log.info(
            { orderId, reason: result.reason, alreadyRejected: result.duplicate },
            'inventory rejected',
          );
          break;
        case 'skipped':
          log.info(
            { orderId, status: result.status },
            'order already cancelled - nothing reserved',
          );
          break;
      }
    },

    [Topics.PaymentsFailed]: async (event: PaymentFailedEvent, { log }: HandlerContext) => {
      const { orderId, reason } = event.data;
      const result = await inventory.release(orderId, `payment failed: ${reason}`, causeOf(event));
      relay.poke();

      if (result.outcome === 'released') {
        log.info({ orderId, items: result.items }, 'payment failed -> reservation released');
      } else {
        log.debug({ orderId, result }, 'payment failed -> no stock to release');
      }
    },
  };
}
