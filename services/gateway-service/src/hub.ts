import { Scopes } from '@orderflow/auth';
import type { FeedEntry, SnapshotPayload, StockView } from '@orderflow/stream-types';

export type { FeedEntry, SnapshotPayload, StockView };

export interface Principal {
  sub: string;
  scopes: ReadonlySet<string>;
}

/** One SSE message. `id` (feed position) lets a reconnecting client resume via Last-Event-ID. */
export interface Frame {
  id?: string;
  event: 'snapshot' | 'stock' | 'feed' | 'flags' | 'ping';
  data: unknown;
}

export interface StockUpdate {
  sku: string;
  name: string;
  available: number;
  lowStockThreshold: number;
  version: number;
  occurredAt: string;
  delta: number;
  reason: string;
  orderId?: string;
}

export type NewFeedEntry = Omit<FeedEntry, 'seq'> & {
  /** Who placed the order (event actor). Only that user - and admins - see order entries. */
  ownerSub?: string;
};

export const canSeeStock = (p: Principal) =>
  p.scopes.has(Scopes.InventoryRead) || p.scopes.has(Scopes.InventoryWrite);

export const canSeeOrders = (p: Principal) => p.scopes.has(Scopes.OrdersRead);

interface Stored {
  entry: FeedEntry;
  ownerSub?: string;
}

interface Subscriber {
  principal: Principal;
  send: (frame: Frame) => void;
}

export interface HubOptions {
  /** Identifies this gateway run. Feed ids from another run can't be resumed from. */
  epoch?: string;
  /** How many feed entries are kept for new and reconnecting clients. */
  bufferSize?: number;
  /** How many entries a brand-new client gets. */
  recentCount?: number;
  now?: () => Date;
}

/**
 * The gateway's read model and fan-out point.
 *
 * - **Stock** is *state*: the latest level per SKU, rebuilt from the compacted
 *   `inventory.stock-levels` topic. Every connect gets the whole table, so stock never
 *   needs replaying and a reconnecting client can't miss a change.
 * - **Feed** is *events*: orders, payments, reservations and alerts. A bounded ring buffer
 *   keeps the latest ones so a client that drops its connection resumes from its
 *   Last-Event-ID instead of losing what happened in between.
 *
 * Who sees what is decided here, per principal (see {@link canSeeStock}, {@link canSeeOrders}).
 */
export class Hub {
  readonly epoch: string;
  private readonly bufferSize: number;
  private readonly recentCount: number;
  private readonly now: () => Date;
  private readonly stock = new Map<string, StockView>();
  private readonly buffer: Stored[] = [];
  private readonly subscribers = new Set<Subscriber>();
  private seq = 0;

  constructor(options: HubOptions = {}) {
    this.epoch = options.epoch ?? Date.now().toString(36);
    this.bufferSize = options.bufferSize ?? 500;
    this.recentCount = options.recentCount ?? 50;
    this.now = options.now ?? (() => new Date());
  }

  get clients(): number {
    return this.subscribers.size;
  }

  get skuCount(): number {
    return this.stock.size;
  }

  get lastSeq(): number {
    return this.seq;
  }

  /**
   * Applies a stock level. Updates older than what we have are ignored (`version` orders
   * changes per SKU even if records arrive out of order), and so are resyncs that change
   * nothing. Returns true if the table changed.
   */
  applyStock(update: StockUpdate): boolean {
    const current = this.stock.get(update.sku);
    if (current) {
      if (update.version < current.version) return false;
      if (update.version === current.version && update.available === current.available) {
        return false;
      }
    }
    const view: StockView = {
      sku: update.sku,
      name: update.name,
      available: update.available,
      lowStockThreshold: update.lowStockThreshold,
      low: update.available <= update.lowStockThreshold,
      version: update.version,
      updatedAt: update.occurredAt,
      lastChange: {
        delta: update.delta,
        reason: update.reason,
        ...(update.orderId && { orderId: update.orderId }),
      },
    };
    this.stock.set(view.sku, view);
    this.broadcast({ event: 'stock', data: view }, (p) => canSeeStock(p));
    return true;
  }

  publishFeed(input: NewFeedEntry): FeedEntry {
    const { ownerSub, ...rest } = input;
    const entry: FeedEntry = { ...rest, seq: ++this.seq };
    this.buffer.push({ entry, ...(ownerSub && { ownerSub }) });
    if (this.buffer.length > this.bufferSize) this.buffer.shift();

    this.broadcast({ id: this.frameId(entry.seq), event: 'feed', data: entry }, (p) =>
      this.canSeeEntry(p, { entry, ownerSub }),
    );
    return entry;
  }

  /** What a client with this identity sees right now. */
  snapshot(principal: Principal, lastEventId?: string): SnapshotPayload {
    const resumeFrom = this.parseLastEventId(lastEventId);
    const visible = this.buffer.filter((s) => this.canSeeEntry(principal, s));
    const feed =
      resumeFrom === undefined
        ? visible.slice(-this.recentCount)
        : visible.filter((s) => s.entry.seq > resumeFrom);
    return {
      stock: canSeeStock(principal) ? [...this.stock.values()].sort(bySku) : null,
      feed: feed.map((s) => s.entry),
      resumed: resumeFrom !== undefined,
      serverTime: this.now().toISOString(),
    };
  }

  /**
   * Registers a client: sends one `snapshot` frame, then live `stock` / `feed` frames until
   * the returned function is called. Snapshot and registration happen in the same tick, so
   * nothing can slip in between.
   */
  connect(principal: Principal, send: (frame: Frame) => void, lastEventId?: string): () => void {
    send({
      id: this.frameId(this.seq),
      event: 'snapshot',
      data: this.snapshot(principal, lastEventId),
    });
    const subscriber: Subscriber = { principal, send };
    this.subscribers.add(subscriber);
    return () => {
      this.subscribers.delete(subscriber);
    };
  }

  private frameId(seq: number): string {
    return `${this.epoch}:${seq}`;
  }

  /** Position to resume after, or undefined if the id is absent, foreign, or already evicted. */
  private parseLastEventId(lastEventId: string | undefined): number | undefined {
    const match = /^([^:]+):(\d+)$/.exec(lastEventId ?? '');
    if (!match || match[1] !== this.epoch) return undefined;
    const seq = Number(match[2]);
    if (seq > this.seq) return undefined;
    const oldest = this.buffer[0]?.entry.seq ?? this.seq + 1;
    // Entries in (seq, oldest) were evicted: resuming would silently skip them.
    return seq + 1 >= oldest ? seq : undefined;
  }

  private canSeeEntry(principal: Principal, { entry, ownerSub }: Stored): boolean {
    if (entry.kind === 'stock-alert') return canSeeStock(principal);
    if (!canSeeOrders(principal)) return false;
    return (
      principal.scopes.has(Scopes.Admin) || (ownerSub !== undefined && ownerSub === principal.sub)
    );
  }

  private broadcast(frame: Frame, allowed: (principal: Principal) => boolean): void {
    for (const subscriber of [...this.subscribers]) {
      if (!allowed(subscriber.principal)) continue;
      try {
        subscriber.send(frame);
      } catch {
        this.subscribers.delete(subscriber); // a broken connection must not stop the others
      }
    }
  }
}

const bySku = (a: StockView, b: StockView) => a.sku.localeCompare(b.sku);
