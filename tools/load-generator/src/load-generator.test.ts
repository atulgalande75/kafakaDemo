import { describe, expect, it } from 'vitest';
import { Topics, decodeEvent } from '@orderflow/contracts';
import { poisonMessages } from './poison.js';
import { randomOrder } from './scenarios.js';
import { percentile, summarize } from './stats.js';

describe('randomOrder', () => {
  it('produces valid, in-stock orders for the happy scenario', () => {
    for (let i = 0; i < 50; i++) {
      const order = randomOrder('happy');
      expect(order.items.length).toBeGreaterThan(0);
      expect(order.items.every((i) => i.sku !== 'SKU-GPU' && i.quantity > 0)).toBe(true);
    }
  });

  it('includes failure cases in the mixed scenario', () => {
    expect(randomOrder('mixed', () => 0.1).items[0]?.sku).toBe('SKU-GPU');
    expect(randomOrder('mixed', () => 0.2).items).toEqual([
      { sku: 'SKU-LAPTOP', quantity: 2, unitPrice: 1499 },
    ]);
  });
});

describe('poisonMessages', () => {
  it('generates messages that all fail decoding', () => {
    const messages = poisonMessages(5);
    expect(messages).toHaveLength(5);
    for (const m of messages) {
      expect(() => decodeEvent(Topics.OrdersCreated, m.value as Buffer | string)).toThrow();
    }
  });
});

describe('stats', () => {
  it('computes percentiles', () => {
    const values = Array.from({ length: 100 }, (_, i) => i + 1);
    expect(percentile(values, 50)).toBe(50);
    expect(percentile(values, 95)).toBe(95);
    expect(percentile([], 95)).toBe(0);
  });

  it('summarizes settled orders', () => {
    const at = (ms: number) => new Date(ms).toISOString();
    const summary = summarize([
      {
        status: 'CONFIRMED',
        createdAt: at(0),
        updatedAt: at(100),
        payment: { status: 'COMPLETED' },
        inventory: { status: 'RESERVED' },
      },
      {
        status: 'CANCELLED',
        createdAt: at(0),
        updatedAt: at(300),
        payment: { status: 'FAILED', reason: 'Amount 2998 USD exceeds card limit of 2000' },
        inventory: { status: 'RESERVED' },
      },
      {
        status: 'PENDING',
        createdAt: at(0),
        updatedAt: at(0),
        payment: { status: 'PENDING' },
        inventory: { status: 'PENDING' },
      },
    ]);
    expect(summary.byStatus).toEqual({ CONFIRMED: 1, CANCELLED: 1, PENDING: 1 });
    expect(summary.cancelReasons).toEqual({ 'Amount N USD exceeds card limit of N': 1 });
    expect(summary.latencyMs).toEqual({ p50: 100, p95: 300, max: 300 });
  });
});
