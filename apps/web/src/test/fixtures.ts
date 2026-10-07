import type { FeedEntry, StockView } from '@orderflow/stream-types';

export const stockView = (over: Partial<StockView> = {}): StockView => ({
  sku: 'SKU-A',
  name: 'Alpha',
  available: 50,
  lowStockThreshold: 5,
  low: false,
  version: 1,
  updatedAt: '2026-01-01T10:00:00.000Z',
  lastChange: { delta: 0, reason: 'snapshot' },
  ...over,
});

export const feedEntry = (over: Partial<FeedEntry> = {}): FeedEntry => ({
  seq: 1,
  kind: 'order',
  type: 'order.created',
  occurredAt: '2026-01-01T10:00:00.000Z',
  correlationId: 'c',
  summary: 'Order 11111111 placed',
  orderId: '11111111-1111-1111-1111-111111111111',
  data: {},
  ...over,
});
