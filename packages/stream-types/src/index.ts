/**
 * What gateway-service sends over `GET /stream` (Server-Sent Events) and `GET /snapshot`.
 * Types only: this package has no runtime code, so a browser bundle can import it freely.
 */

/** The SSE `event:` names. */
/**
 * `ping` is a heartbeat (empty data) sent every few seconds, so a client can tell a quiet
 * stream from a dead connection that an intermediary left open.
 */
export type StreamEventName = 'snapshot' | 'stock' | 'feed' | 'flags' | 'ping';

export interface StockView {
  sku: string;
  name: string;
  available: number;
  lowStockThreshold: number;
  low: boolean;
  /** Increases with every change to the SKU, so an older update can be ignored. */
  version: number;
  updatedAt: string;
  /** The most recent change (delta 0 / reason "snapshot" if only a resync was seen). */
  lastChange: { delta: number; reason: string; orderId?: string };
}

/** One line of the live activity feed. */
export interface FeedEntry {
  /** Position in the gateway's feed; also the numeric part of the SSE id (`<epoch>:<seq>`). */
  seq: number;
  kind: 'order' | 'stock-alert';
  /** Event type, e.g. `order.created`, `payment.failed`, `stock.low`. */
  type: string;
  occurredAt: string;
  correlationId: string;
  summary: string;
  orderId?: string;
  sku?: string;
  data: unknown;
}

/** First frame of every connection (and the body of `GET /snapshot`). */
export interface SnapshotPayload {
  /** Full stock table, or null if the caller may not see stock. */
  stock: StockView[] | null;
  /** Feed entries the caller may see: the recent ones, or only the missed ones if `resumed`. */
  feed: FeedEntry[];
  /** True when the client's Last-Event-ID was understood and `feed` only holds what it missed. */
  resumed: boolean;
  serverTime: string;
}

/**
 * The feature flags the web app uses, evaluated by the gateway for the signed-in user
 * (`GET /flags`, a `flags` stream frame whenever one changes, and `GET /snapshot`).
 */
export interface UiFlags {
  /** false: the web app polls `/snapshot` instead of holding an SSE connection. */
  liveUpdates: boolean;
  /** Show stock as a grid of cards instead of a table. */
  newInventoryDashboard: boolean;
  /** Show the "restock all low items" action. */
  bulkAdjust: boolean;
  /** How many entries the live activity feed shows. */
  activityFeedSize: number;
}

/** `GET /snapshot`: the stream's first frame, plus the user's flags (for polling clients). */
export type SnapshotResponse = SnapshotPayload & { flags: UiFlags };
