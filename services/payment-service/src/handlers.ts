import {
  EventTypes,
  Topics,
  createEvent,
  deriveEventId,
  type OrderCreatedEvent,
} from '@orderflow/contracts';
import type { EventHandlers, EventProducer, HandlerContext } from '@orderflow/kafka-utils';
import { chargeCard, type PaymentSettings } from './payment.js';

export function createPaymentHandlers(
  producer: Pick<EventProducer, 'publish'>,
  settings: PaymentSettings,
  random: () => number = Math.random,
): EventHandlers {
  return {
    [Topics.OrdersCreated]: async (event: OrderCreatedEvent, { log, attempt }: HandlerContext) => {
      const { orderId, totalAmount, currency } = event.data;
      const decision = chargeCard(event.data, settings, random);
      const options = {
        correlationId: event.correlationId,
        eventId: deriveEventId(event.eventId, 'payment'),
      };

      if (decision.approved) {
        const paymentId = deriveEventId(event.eventId, 'payment-id');
        await producer.publish(
          Topics.PaymentsCompleted,
          createEvent(
            EventTypes.PaymentCompleted,
            { orderId, paymentId, amount: totalAmount, currency },
            options,
          ),
          { key: orderId },
        );
        log.info({ orderId, amount: totalAmount, attempt }, 'payment completed');
      } else {
        await producer.publish(
          Topics.PaymentsFailed,
          createEvent(EventTypes.PaymentFailed, { orderId, reason: decision.reason }, options),
          { key: orderId },
        );
        log.info({ orderId, reason: decision.reason, attempt }, 'payment failed');
      }
    },
  };
}
