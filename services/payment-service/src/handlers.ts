import {
  EventTypes,
  Topics,
  createEvent,
  deriveEventId,
  type OrderCreatedEvent,
} from '@orderflow/contracts';
import { orderContext, type FeatureFlags } from '@orderflow/feature-flags';
import type { EventHandlers, EventProducer, HandlerContext } from '@orderflow/kafka-utils';
import { fraudCheck } from './fraud.js';
import { chargeCard, type PaymentSettings } from './payment.js';

export type StaticPaymentSettings = Omit<PaymentSettings, 'failureRate'>;

export function createPaymentHandlers(
  producer: Pick<EventProducer, 'publish'>,
  flags: Pick<FeatureFlags, 'get'>,
  settings: StaticPaymentSettings,
  random: () => number = Math.random,
): EventHandlers {
  return {
    [Topics.OrdersCreated]: async (event: OrderCreatedEvent, { log, attempt }: HandlerContext) => {
      const { orderId, totalAmount, currency } = event.data;
      const options = {
        correlationId: event.correlationId,
        actor: event.actor,
        eventId: deriveEventId(event.eventId, 'payment'),
      };
      const publishFailed = async (reason: string) => {
        await producer.publish(
          Topics.PaymentsFailed,
          createEvent(EventTypes.PaymentFailed, { orderId, reason }, options),
          { key: orderId },
        );
        log.info({ orderId, reason, attempt }, 'payment failed');
      };

      // Flags are evaluated per order, so LaunchDarkly can target by tier or country.
      const context = orderContext(event.data);
      const [failureRate, fraudCheckEnabled] = await Promise.all([
        flags.get('payment-failure-rate', context),
        flags.get('fraud-check-enabled', context),
      ]);

      if (fraudCheckEnabled) {
        const fraud = fraudCheck(event.data);
        log.info({ orderId, passed: fraud.passed }, 'fraud check');
        if (!fraud.passed) return publishFailed(`Fraud check failed: ${fraud.reason}`);
      }

      const decision = chargeCard(event.data, { ...settings, failureRate }, random);
      if (!decision.approved) return publishFailed(decision.reason);

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
    },
  };
}
