import type { LDContext } from '@launchdarkly/node-server-sdk';
import type { AnyEvent } from '@orderflow/contracts';

/**
 * What a flag is evaluated for. Most flags are evaluated per order so LaunchDarkly
 * can target by customer tier or country (or roll out to a percentage of orders);
 * service-wide switches use a service context.
 */
export type FlagContext =
  | { kind: 'order'; orderId: string; customerTier?: string; country?: string }
  | { kind: 'service'; service: string }
  /** A signed-in person (the web app's flags): key is the token's `sub`. */
  | { kind: 'user'; sub: string; username?: string };

export function orderContext(order: {
  orderId: string;
  customerTier?: string;
  country?: string;
}): FlagContext {
  return {
    kind: 'order',
    orderId: order.orderId,
    customerTier: order.customerTier,
    country: order.country,
  };
}

export function userContext(sub: string, username?: string): FlagContext {
  return { kind: 'user', sub, ...(username && { username }) };
}

export function serviceContext(service: string): FlagContext {
  return { kind: 'service', service };
}

/**
 * Order context for any pipeline event (tier and country are only on orders.created).
 * Events that aren't about an order (stock events) get a service context.
 */
export function eventContext(event: AnyEvent): FlagContext {
  const data = event.data as { orderId?: string; customerTier?: string; country?: string };
  return data.orderId ? orderContext({ ...data, orderId: data.orderId }) : serviceContext('stock');
}

/** Maps our context to a LaunchDarkly context (kinds "order" and "service"). */
export function toLdContext(context: FlagContext): LDContext {
  if (context.kind === 'service') return { kind: 'service', key: context.service };
  if (context.kind === 'user') {
    return { kind: 'user', key: context.sub, ...(context.username && { name: context.username }) };
  }
  return {
    kind: 'order',
    key: context.orderId,
    ...(context.customerTier && { customerTier: context.customerTier }),
    ...(context.country && { country: context.country }),
  };
}
