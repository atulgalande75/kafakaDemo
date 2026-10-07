/** Prices the order form uses (the order API takes a unit price per line). Stock names come from the stream. */
export const CATALOG = [
  { sku: 'SKU-KEYBOARD', name: 'Mechanical keyboard', price: 79.99 },
  { sku: 'SKU-MOUSE', name: 'Wireless mouse', price: 25 },
  { sku: 'SKU-MONITOR', name: '27" monitor', price: 249 },
  { sku: 'SKU-LAPTOP', name: 'Laptop 14"', price: 1299 },
  { sku: 'SKU-HEADSET', name: 'Noise-cancelling headset', price: 89 },
  { sku: 'SKU-WEBCAM', name: 'HD webcam', price: 45 },
  { sku: 'SKU-GPU', name: 'Graphics card', price: 899 },
] as const;

export const COUNTRIES = ['US', 'DE', 'GB', 'BR', 'IN'] as const;
export const TIERS = ['standard', 'gold', 'platinum'] as const;

/** Orders above this amount are declined by payment-service (its PAYMENT_CARD_LIMIT default). */
export const CARD_LIMIT = 2000;
