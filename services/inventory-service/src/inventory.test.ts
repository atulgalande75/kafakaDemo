import { describe, expect, it } from 'vitest';
import { Inventory } from './inventory.js';

describe('Inventory', () => {
  it('reserves all lines and decrements stock', () => {
    const inv = new Inventory({ A: 5, B: 2 });
    const result = inv.reserve('o-1', [
      { sku: 'A', quantity: 2 },
      { sku: 'B', quantity: 1 },
      { sku: 'A', quantity: 1 },
    ]);
    expect(result).toMatchObject({
      ok: true,
      alreadyReserved: false,
      items: [
        { sku: 'A', quantity: 3 },
        { sku: 'B', quantity: 1 },
      ],
    });
    expect(inv.snapshot()).toEqual({ A: 2, B: 1 });
  });

  it('rejects the whole order if any line is short, leaving stock unchanged', () => {
    const inv = new Inventory({ A: 5, B: 0 });
    const result = inv.reserve('o-1', [
      { sku: 'A', quantity: 1 },
      { sku: 'B', quantity: 1 },
    ]);
    expect(result).toEqual({
      ok: false,
      reason: 'Insufficient stock for B',
      unavailable: [{ sku: 'B', requested: 1, available: 0 }],
    });
    expect(inv.snapshot()).toEqual({ A: 5, B: 0 });
  });

  it('rejects unknown SKUs', () => {
    const result = new Inventory({ A: 1 }).reserve('o-1', [{ sku: 'NOPE', quantity: 1 }]);
    expect(result).toMatchObject({ ok: false, reason: 'Unknown SKU(s): NOPE' });
  });

  it('never reserves twice for the same order', () => {
    const inv = new Inventory({ A: 5 });
    const first = inv.reserve('o-1', [{ sku: 'A', quantity: 2 }]);
    const second = inv.reserve('o-1', [{ sku: 'A', quantity: 2 }]);
    expect(second).toMatchObject({ ok: true, alreadyReserved: true });
    expect(first.ok && second.ok && second.reservationId === first.reservationId).toBe(true);
    expect(inv.available('A')).toBe(3);
  });
});
