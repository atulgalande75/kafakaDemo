import type { CustomerTier, OrderCreatedEvent } from '@orderflow/contracts';

/** Largest order amount each tier may place without manual review. */
export const FRAUD_LIMITS: Record<CustomerTier, number> = {
  standard: 1000,
  gold: 5000,
  platinum: Infinity,
};

/** "ZZ" is the ISO user-assigned code for an unknown country. */
export const BLOCKED_COUNTRIES: readonly string[] = ['ZZ'];

export type FraudResult = { passed: true } | { passed: false; reason: string };

/**
 * Simulated fraud check, only run when the fraud-check-enabled flag is on for the
 * order. Deliberately simple and deterministic so it is easy to demo.
 */
export function fraudCheck(order: OrderCreatedEvent['data']): FraudResult {
  if (BLOCKED_COUNTRIES.includes(order.country)) {
    return { passed: false, reason: `country ${order.country} is not supported` };
  }
  const limit = FRAUD_LIMITS[order.customerTier];
  if (order.totalAmount > limit) {
    return {
      passed: false,
      reason: `${order.totalAmount} ${order.currency} exceeds the ${order.customerTier} tier limit of ${limit}`,
    };
  }
  return { passed: true };
}
