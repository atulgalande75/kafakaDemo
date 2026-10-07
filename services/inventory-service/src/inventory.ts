import { randomUUID } from 'node:crypto';
import {
  EventTypes,
  Topics,
  createEvent,
  deriveEventId,
  type Actor,
  type StockChangeReason,
} from '@orderflow/contracts';
import type { Db, Queryable } from './db.js';
import { enqueue } from './outbox.js';

export interface CatalogItem {
  sku: string;
  name: string;
  available: number;
  lowStockThreshold: number;
}

/** Starting stock. SKU-WEBCAM runs out quickly under load; SKU-GPU is always out of stock. */
export const INITIAL_CATALOG: readonly CatalogItem[] = [
  { sku: 'SKU-KEYBOARD', name: 'Mechanical keyboard', available: 5_000, lowStockThreshold: 500 },
  { sku: 'SKU-MOUSE', name: 'Wireless mouse', available: 10_000, lowStockThreshold: 1_000 },
  { sku: 'SKU-MONITOR', name: '27" monitor', available: 2_000, lowStockThreshold: 200 },
  { sku: 'SKU-LAPTOP', name: 'Laptop 14"', available: 1_000, lowStockThreshold: 100 },
  {
    sku: 'SKU-HEADSET',
    name: 'Noise-cancelling headset',
    available: 3_000,
    lowStockThreshold: 300,
  },
  { sku: 'SKU-WEBCAM', name: 'HD webcam', available: 10, lowStockThreshold: 5 },
  { sku: 'SKU-GPU', name: 'Graphics card', available: 0, lowStockThreshold: 5 },
];

export interface ReservationLine {
  sku: string;
  quantity: number;
}

/** What caused a change, copied onto the events it produces. */
export interface Cause {
  /** The Kafka event being processed. Output eventIds derive from it, so a retry re-emits the same ids. */
  eventId?: string;
  correlationId: string;
  actor?: Actor;
}

export interface StockItem {
  sku: string;
  name: string;
  available: number;
  lowStockThreshold: number;
  low: boolean;
  version: number;
  updatedAt: string;
}

export type ReserveResult =
  | { outcome: 'reserved'; reservationId: string; items: ReservationLine[]; duplicate: boolean }
  | {
      outcome: 'rejected';
      reason: string;
      unavailable: Array<{ sku: string; requested: number; available: number }>;
      duplicate: boolean;
    }
  /** The order was already cancelled or released, so there is nothing to reserve. */
  | { outcome: 'skipped'; status: 'released' | 'cancelled' };

export type ReleaseResult =
  | { outcome: 'released'; reservationId: string; items: ReservationLine[] }
  /** Payment failed before we reserved: remember it so a late orders.created is ignored. */
  | { outcome: 'cancelled-in-advance' }
  /** Nothing to give back (rejected, already released or already cancelled). */
  | { outcome: 'nothing-to-release'; status: string };

export class UnknownSkuError extends Error {
  override readonly name = 'UnknownSkuError';
  constructor(readonly sku: string) {
    super(`Unknown SKU: ${sku}`);
  }
}

export class InsufficientStockError extends Error {
  override readonly name = 'InsufficientStockError';
  constructor(
    readonly sku: string,
    readonly available: number,
    readonly delta: number,
  ) {
    super(`Cannot remove ${-delta} of ${sku}: only ${available} available`);
  }
}

interface StockRow {
  sku: string;
  name: string;
  available: number;
  low_stock_threshold: number;
  version: string | number;
  updated_at: Date | string;
}

interface ReservationRow {
  reservation_id: string;
  status: 'reserved' | 'rejected' | 'released' | 'cancelled';
  items: ReservationLine[];
  detail: {
    reason: string;
    unavailable: Extract<ReserveResult, { outcome: 'rejected' }>['unavailable'];
  } | null;
}

const toItem = (row: StockRow): StockItem => ({
  sku: row.sku,
  name: row.name,
  available: row.available,
  lowStockThreshold: row.low_stock_threshold,
  low: row.available <= row.low_stock_threshold,
  version: Number(row.version),
  updatedAt: new Date(row.updated_at).toISOString(),
});

const STOCK_COLUMNS = 'sku, name, available, low_stock_threshold, version, updated_at';

/**
 * Stock and reservations in Postgres. Every operation is one transaction that changes the
 * rows *and* writes the resulting events to the outbox, so state and events can't diverge.
 * A reservation is all-or-nothing: either every line is available and all are decremented,
 * or nothing changes.
 */
export class InventoryStore {
  constructor(private readonly db: Db) {}

  /** Inserts the initial catalog for SKUs that don't exist yet. Existing stock is never touched. */
  async seed(catalog: readonly CatalogItem[] = INITIAL_CATALOG): Promise<number> {
    let inserted = 0;
    for (const item of catalog) {
      const res = await this.db.query(
        `INSERT INTO stock (sku, name, available, low_stock_threshold) VALUES ($1, $2, $3, $4)
         ON CONFLICT (sku) DO NOTHING`,
        [item.sku, item.name, item.available, item.lowStockThreshold],
      );
      inserted += res.rowCount;
    }
    return inserted;
  }

  /**
   * Publishes the current level of every SKU to inventory.stock-levels (reason `snapshot`).
   * Run at startup: it (re)builds the compacted topic if Kafka was reset, and compaction
   * keeps only the newest record per SKU anyway.
   */
  async publishSnapshot(): Promise<number> {
    return this.db.transaction(async (tx) => {
      const { rows } = await tx.query<StockRow>(
        `SELECT ${STOCK_COLUMNS} FROM stock ORDER BY sku FOR SHARE`,
      );
      for (const row of rows) {
        await enqueue(
          tx,
          Topics.StockLevels,
          row.sku,
          createEvent(
            EventTypes.StockLevelChanged,
            {
              sku: row.sku,
              name: row.name,
              available: row.available,
              previousAvailable: row.available,
              delta: 0,
              reason: 'snapshot',
              version: Number(row.version),
              lowStockThreshold: row.low_stock_threshold,
            },
            { correlationId: `snapshot-${randomUUID()}` },
          ),
        );
      }
      return rows.length;
    });
  }

  async list(): Promise<StockItem[]> {
    const { rows } = await this.db.query<StockRow>(
      `SELECT ${STOCK_COLUMNS} FROM stock ORDER BY sku`,
    );
    return rows.map(toItem);
  }

  async get(sku: string): Promise<StockItem | undefined> {
    const { rows } = await this.db.query<StockRow>(
      `SELECT ${STOCK_COLUMNS} FROM stock WHERE sku = $1`,
      [sku],
    );
    return rows[0] && toItem(rows[0]);
  }

  async reserve(orderId: string, items: ReservationLine[], cause: Cause): Promise<ReserveResult> {
    const requested = new Map<string, number>();
    for (const { sku, quantity } of items) {
      requested.set(sku, (requested.get(sku) ?? 0) + quantity);
    }

    return this.db.transaction(async (tx) => {
      await lockOrder(tx, orderId);
      const existing = await findReservation(tx, orderId);
      if (existing) return fromStoredReservation(existing);

      const stock = await lockStock(tx, [...requested.keys()]);
      const unavailable = [...requested]
        .map(([sku, quantity]) => ({
          sku,
          requested: quantity,
          available: stock.get(sku)?.available ?? 0,
        }))
        .filter((line) => line.available < line.requested);

      const reservationId = randomUUID();
      if (unavailable.length > 0) {
        const unknown = unavailable.filter((u) => !stock.has(u.sku)).map((u) => u.sku);
        const reason =
          unknown.length > 0
            ? `Unknown SKU(s): ${unknown.join(', ')}`
            : `Insufficient stock for ${unavailable.map((u) => u.sku).join(', ')}`;
        await tx.query(
          `INSERT INTO reservations (order_id, reservation_id, status, detail)
           VALUES ($1, $2, 'rejected', $3::jsonb)`,
          [orderId, reservationId, JSON.stringify({ reason, unavailable })],
        );
        await enqueue(
          tx,
          Topics.InventoryRejected,
          orderId,
          createEvent(
            EventTypes.InventoryRejected,
            { orderId, reason, unavailable },
            eventOptions(cause, 'inventory'),
          ),
        );
        return { outcome: 'rejected', reason, unavailable, duplicate: false };
      }

      const lines = [...requested].map(([sku, quantity]) => ({ sku, quantity }));
      await tx.query(
        `INSERT INTO reservations (order_id, reservation_id, status, items)
         VALUES ($1, $2, 'reserved', $3::jsonb)`,
        [orderId, reservationId, JSON.stringify(lines)],
      );
      for (const { sku, quantity } of lines) {
        await applyChange(tx, stock.get(sku)!, -quantity, 'reserved', cause, { orderId });
      }
      await enqueue(
        tx,
        Topics.InventoryReserved,
        orderId,
        createEvent(
          EventTypes.InventoryReserved,
          { orderId, reservationId, items: lines },
          eventOptions(cause, 'inventory'),
        ),
      );
      return { outcome: 'reserved', reservationId, items: lines, duplicate: false };
    });
  }

  /** Gives a reservation back to stock, e.g. because the order's payment failed. */
  async release(orderId: string, reason: string, cause: Cause): Promise<ReleaseResult> {
    return this.db.transaction(async (tx) => {
      await lockOrder(tx, orderId);
      const existing = await findReservation(tx, orderId);

      if (!existing) {
        await tx.query(
          `INSERT INTO reservations (order_id, reservation_id, status) VALUES ($1, $2, 'cancelled')`,
          [orderId, randomUUID()],
        );
        return { outcome: 'cancelled-in-advance' };
      }
      if (existing.status !== 'reserved') {
        return { outcome: 'nothing-to-release', status: existing.status };
      }

      const stock = await lockStock(
        tx,
        existing.items.map((i) => i.sku),
      );
      for (const { sku, quantity } of existing.items) {
        await applyChange(tx, stock.get(sku)!, quantity, 'released', cause, { orderId });
      }
      await tx.query(
        `UPDATE reservations SET status = 'released', released_at = now() WHERE order_id = $1`,
        [orderId],
      );
      await enqueue(
        tx,
        Topics.InventoryReleased,
        orderId,
        createEvent(
          EventTypes.InventoryReleased,
          {
            orderId,
            reservationId: existing.reservation_id,
            items: existing.items,
            reason,
          },
          eventOptions(cause, 'release'),
        ),
      );
      return {
        outcome: 'released',
        reservationId: existing.reservation_id,
        items: existing.items,
      };
    });
  }

  /** A manual stock change (restock, shrinkage, correction). Stock can't go below zero. */
  async adjust(
    sku: string,
    delta: number,
    reason: Extract<StockChangeReason, 'restock' | 'shrinkage' | 'correction'>,
    cause: Cause,
    note?: string,
  ): Promise<StockItem> {
    return this.db.transaction(async (tx) => {
      const row = (await lockStock(tx, [sku])).get(sku);
      if (!row) throw new UnknownSkuError(sku);
      if (row.available + delta < 0) throw new InsufficientStockError(sku, row.available, delta);
      return applyChange(tx, row, delta, reason, cause, { note });
    });
  }
}

/** Serializes work on one order for the duration of the transaction. */
async function lockOrder(tx: Queryable, orderId: string): Promise<void> {
  await tx.query('SELECT pg_advisory_xact_lock(hashtextextended($1, 0))', [orderId]);
}

async function findReservation(
  tx: Queryable,
  orderId: string,
): Promise<ReservationRow | undefined> {
  const { rows } = await tx.query<ReservationRow>(
    'SELECT reservation_id, status, items, detail FROM reservations WHERE order_id = $1',
    [orderId],
  );
  return rows[0];
}

function fromStoredReservation(row: ReservationRow): ReserveResult {
  switch (row.status) {
    case 'reserved':
      return {
        outcome: 'reserved',
        reservationId: row.reservation_id,
        items: row.items,
        duplicate: true,
      };
    case 'rejected':
      return {
        outcome: 'rejected',
        reason: row.detail?.reason ?? 'Rejected',
        unavailable: row.detail?.unavailable ?? [],
        duplicate: true,
      };
    default:
      return { outcome: 'skipped', status: row.status };
  }
}

/** Locks the stock rows in SKU order (so concurrent transactions can't deadlock). */
async function lockStock(tx: Queryable, skus: string[]): Promise<Map<string, StockRow>> {
  const { rows } = await tx.query<StockRow>(
    `SELECT ${STOCK_COLUMNS} FROM stock WHERE sku = ANY($1::text[]) ORDER BY sku FOR UPDATE`,
    [skus],
  );
  return new Map(rows.map((r) => [r.sku, r]));
}

function eventOptions(cause: Cause, name: string) {
  return {
    correlationId: cause.correlationId,
    ...(cause.actor && { actor: cause.actor }),
    eventId: cause.eventId ? deriveEventId(cause.eventId, name) : randomUUID(),
  };
}

/**
 * Changes one SKU's stock by `delta` and enqueues stock.level.changed (plus stock.low when
 * the level just dropped to or below its threshold). `row` must be locked by the caller.
 */
async function applyChange(
  tx: Queryable,
  row: StockRow,
  delta: number,
  reason: StockChangeReason,
  cause: Cause,
  extra: { orderId?: string; note?: string } = {},
): Promise<StockItem> {
  const available = row.available + delta;
  const version = Number(row.version) + 1;
  const { rows } = await tx.query<StockRow>(
    `UPDATE stock SET available = $2, version = $3, updated_at = now() WHERE sku = $1
     RETURNING ${STOCK_COLUMNS}`,
    [row.sku, available, version],
  );

  await enqueue(
    tx,
    Topics.StockLevels,
    row.sku,
    createEvent(
      EventTypes.StockLevelChanged,
      {
        sku: row.sku,
        name: row.name,
        available,
        previousAvailable: row.available,
        delta,
        reason,
        version,
        lowStockThreshold: row.low_stock_threshold,
        ...(extra.orderId && { orderId: extra.orderId }),
        ...(extra.note && { note: extra.note }),
      },
      eventOptions(cause, `stock/${row.sku}`),
    ),
  );

  if (row.available > row.low_stock_threshold && available <= row.low_stock_threshold) {
    await enqueue(
      tx,
      Topics.StockLow,
      row.sku,
      createEvent(
        EventTypes.StockLow,
        { sku: row.sku, name: row.name, available, threshold: row.low_stock_threshold },
        eventOptions(cause, `stock-low/${row.sku}`),
      ),
    );
  }
  return toItem(rows[0]!);
}
