import { randomUUID } from 'node:crypto';

/** Starting stock. SKU-WEBCAM runs out quickly under load; SKU-GPU is always out of stock. */
export const INITIAL_STOCK: Readonly<Record<string, number>> = {
  'SKU-KEYBOARD': 500,
  'SKU-MOUSE': 1000,
  'SKU-MONITOR': 200,
  'SKU-LAPTOP': 100,
  'SKU-HEADSET': 300,
  'SKU-WEBCAM': 10,
  'SKU-GPU': 0,
};

export interface ReservationLine {
  sku: string;
  quantity: number;
}

export type ReservationResult =
  | { ok: true; reservationId: string; items: ReservationLine[]; alreadyReserved: boolean }
  | {
      ok: false;
      reason: string;
      unavailable: Array<{ sku: string; requested: number; available: number }>;
    };

/**
 * In-memory stock ledger. A reservation is all-or-nothing: either every line is
 * available and all are decremented, or nothing changes.
 */
export class Inventory {
  private readonly stock: Map<string, number>;
  private readonly reservations = new Map<
    string,
    { reservationId: string; items: ReservationLine[] }
  >();

  constructor(initial: Readonly<Record<string, number>> = INITIAL_STOCK) {
    this.stock = new Map(Object.entries(initial));
  }

  reserve(orderId: string, items: ReservationLine[]): ReservationResult {
    // Business-level idempotency as a second line of defence: one reservation per order.
    const existing = this.reservations.get(orderId);
    if (existing) return { ok: true, ...existing, alreadyReserved: true };

    const requested = new Map<string, number>();
    for (const { sku, quantity } of items) {
      requested.set(sku, (requested.get(sku) ?? 0) + quantity);
    }

    const unavailable = [...requested]
      .map(([sku, quantity]) => ({ sku, requested: quantity, available: this.stock.get(sku) ?? 0 }))
      .filter((line) => line.available < line.requested);

    if (unavailable.length > 0) {
      const unknown = unavailable.filter((u) => !this.stock.has(u.sku)).map((u) => u.sku);
      const reason =
        unknown.length > 0
          ? `Unknown SKU(s): ${unknown.join(', ')}`
          : `Insufficient stock for ${unavailable.map((u) => u.sku).join(', ')}`;
      return { ok: false, reason, unavailable };
    }

    for (const [sku, quantity] of requested) {
      this.stock.set(sku, (this.stock.get(sku) ?? 0) - quantity);
    }
    const reservation = {
      reservationId: randomUUID(),
      items: [...requested].map(([sku, quantity]) => ({ sku, quantity })),
    };
    this.reservations.set(orderId, reservation);
    return { ok: true, ...reservation, alreadyReserved: false };
  }

  available(sku: string): number {
    return this.stock.get(sku) ?? 0;
  }

  snapshot(): Record<string, number> {
    return Object.fromEntries(this.stock);
  }
}
