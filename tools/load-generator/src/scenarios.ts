export interface OrderRequest {
  customerId: string;
  items: Array<{ sku: string; quantity: number; unitPrice: number }>;
}

export type Scenario = 'happy' | 'mixed';
export const SCENARIOS: readonly Scenario[] = ['happy', 'mixed'];

const CATALOG = {
  'SKU-KEYBOARD': 79.99,
  'SKU-MOUSE': 24.5,
  'SKU-MONITOR': 229,
  'SKU-HEADSET': 59.9,
  'SKU-LAPTOP': 1499,
  'SKU-WEBCAM': 89,
  'SKU-GPU': 899,
} as const;

type Sku = keyof typeof CATALOG;
const IN_STOCK_CHEAP: Sku[] = ['SKU-KEYBOARD', 'SKU-MOUSE', 'SKU-MONITOR', 'SKU-HEADSET'];

const line = (sku: Sku, quantity: number) => ({ sku, quantity, unitPrice: CATALOG[sku] });

/**
 * Builds a random order for a scenario:
 *  - happy: cheap, in-stock items (only PAYMENT_DECLINE_RATE causes cancellations)
 *  - mixed: ~15% out of stock (SKU-GPU), ~15% over the card limit (2x SKU-LAPTOP)
 */
export function randomOrder(scenario: Scenario, random: () => number = Math.random): OrderRequest {
  const pick = <T>(list: readonly T[]): T => list[Math.floor(random() * list.length)]!;
  const customerId = `customer-${1 + Math.floor(random() * 50)}`;

  if (scenario === 'mixed') {
    const roll = random();
    if (roll < 0.15) return { customerId, items: [line('SKU-GPU', 1), line('SKU-MOUSE', 1)] };
    if (roll < 0.3) return { customerId, items: [line('SKU-LAPTOP', 2)] };
  }

  const count = 1 + Math.floor(random() * 3);
  const skus = new Set<Sku>();
  while (skus.size < count) skus.add(pick(IN_STOCK_CHEAP));
  return { customerId, items: [...skus].map((sku) => line(sku, 1 + Math.floor(random() * 3))) };
}
