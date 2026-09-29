import type { OrderCreatedEvent } from '@orderflow/contracts';

export interface PaymentSettings {
  /**
   * Probability (0-1) that the gateway call throws a transient error - the chaos
   * toggle, read per order from the payment-failure-rate feature flag.
   */
  failureRate: number;
  /** Probability (0-1) that the card is declined (a business outcome, not an error). */
  declineRate: number;
  /** Orders above this amount are always declined - handy for deterministic demos. */
  cardLimit: number;
}

export type PaymentDecision = { approved: true } | { approved: false; reason: string };

/** Thrown by the simulated gateway; retried by the consumer wrapper, then DLQ'd. */
export class PaymentGatewayError extends Error {
  override readonly name = 'PaymentGatewayError';
}

/**
 * Simulates calling a payment gateway. With payment-failure-rate > 0 some calls
 * fail with a transient error to exercise retries, backoff and the DLQ.
 */
export function chargeCard(
  order: OrderCreatedEvent['data'],
  settings: PaymentSettings,
  random: () => number = Math.random,
): PaymentDecision {
  if (random() < settings.failureRate) {
    throw new PaymentGatewayError('Payment gateway timeout (chaos: payment-failure-rate flag)');
  }
  if (order.totalAmount > settings.cardLimit) {
    return {
      approved: false,
      reason: `Amount ${order.totalAmount} ${order.currency} exceeds card limit of ${settings.cardLimit}`,
    };
  }
  if (random() < settings.declineRate) {
    return { approved: false, reason: 'Card declined by issuer' };
  }
  return { approved: true };
}
