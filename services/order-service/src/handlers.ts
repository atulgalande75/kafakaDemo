import { Topics } from '@orderflow/contracts';
import { NonRetryableError, type EventHandlers, type HandlerContext } from '@orderflow/kafka-utils';
import { applyOutcome, type OutcomeEvent } from './order.js';
import type { OrderRepository } from './repository.js';

/** Consumes payment and inventory outcomes and advances the order state machine. */
export function createOutcomeHandlers(repo: OrderRepository): EventHandlers {
  const handle = async (event: OutcomeEvent, { log }: HandlerContext) => {
    const stored = await repo.get(event.data.orderId);
    if (!stored) {
      // Orders are saved before orders.created is published, so this means the order
      // is genuinely unknown (e.g. lost on restart with the in-memory store).
      throw new NonRetryableError(`Unknown order ${event.data.orderId}`);
    }
    const before = stored.order;
    const after = applyOutcome(before, event);
    await repo.save({ ...stored, order: after });

    if (after.status !== before.status) {
      log.info(
        { orderId: after.id, from: before.status, to: after.status, trigger: event.type },
        `order ${after.status}`,
      );
    } else {
      log.info({ orderId: after.id, status: after.status, event: event.type }, 'order updated');
    }
    if (
      before.status === 'CANCELLED' &&
      (event.type === 'payment.completed' || event.type === 'inventory.reserved')
    ) {
      log.warn(
        { orderId: after.id, event: event.type },
        'success outcome for a cancelled order - compensation (refund / release stock) needed',
      );
    }
  };

  return {
    [Topics.PaymentsCompleted]: handle,
    [Topics.PaymentsFailed]: handle,
    [Topics.InventoryReserved]: handle,
    [Topics.InventoryRejected]: handle,
  };
}
