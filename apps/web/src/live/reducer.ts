import type { FeedEntry, SnapshotPayload, StockView, UiFlags } from '@orderflow/stream-types';
import { DEFAULT_UI_FLAGS, sameFlags } from '../flags/defaults';

/** `polling`: the live-updates flag is off, so the app asks for a snapshot every few seconds. */
export type ConnectionStatus = 'connecting' | 'live' | 'polling' | 'reconnecting' | 'error';

export const MAX_FEED = 200;

export interface LiveState {
  status: ConnectionStatus;
  /** Why the stream stopped for good (e.g. the token may not stream), if it did. */
  error?: string;
  /** Null until the first snapshot arrives, and for users who may not see stock. */
  stock: Record<string, StockView> | null;
  /** Newest first. */
  feed: FeedEntry[];
  /** Per SKU: a counter that grows each time a live update arrives. Drives the change highlight. */
  pulses: Record<string, number>;
  /** Highest feed position of an order event seen so far: changes whenever an order moves. */
  orderTick: number;
  pulseCounter: number;
  /** The signed-in user's feature flags (safe defaults until the gateway answers). */
  flags: UiFlags;
  /** False until the first answer (or failure), so the app doesn't pick a transport too early. */
  flagsLoaded: boolean;
}

export type LiveAction =
  | { type: 'status'; status: ConnectionStatus; error?: string }
  | { type: 'snapshot'; payload: SnapshotPayload; polled?: boolean }
  | { type: 'flags'; flags: UiFlags }
  | { type: 'stock'; view: StockView }
  | { type: 'feed'; entry: FeedEntry };

export const initialLiveState: LiveState = {
  status: 'connecting',
  stock: null,
  feed: [],
  pulses: {},
  orderTick: 0,
  pulseCounter: 0,
  flags: DEFAULT_UI_FLAGS,
  flagsLoaded: false,
};

/** Merges entries into the feed: de-duplicated by position, newest first, bounded. */
function mergeFeed(current: FeedEntry[], incoming: FeedEntry[]): FeedEntry[] {
  const bySeq = new Map(current.map((e) => [e.seq, e]));
  for (const entry of incoming) bySeq.set(entry.seq, entry);
  return [...bySeq.values()].sort((a, b) => b.seq - a.seq).slice(0, MAX_FEED);
}

const latestOrderSeq = (feed: FeedEntry[], from: number) =>
  feed.reduce((max, e) => (e.kind === 'order' && e.seq > max ? e.seq : max), from);

export function liveReducer(state: LiveState, action: LiveAction): LiveState {
  switch (action.type) {
    case 'status':
      return { ...state, status: action.status, error: action.error };

    case 'flags':
      if (state.flagsLoaded && sameFlags(state.flags, action.flags)) return state;
      return { ...state, flags: action.flags, flagsLoaded: true };

    case 'snapshot': {
      const { payload } = action;
      const nextStock = payload.stock
        ? Object.fromEntries(payload.stock.map((s) => [s.sku, s]))
        : null;
      // Rows that changed while we weren't listening (reconnect, polling) light up too.
      let pulseCounter = state.pulseCounter;
      const pulses = { ...state.pulses };
      for (const [sku, view] of Object.entries(nextStock ?? {})) {
        const before = state.stock?.[sku];
        if (before && view.version > before.version) pulses[sku] = ++pulseCounter;
      }
      // A resumed snapshot only carries what was missed; a fresh one replaces everything.
      const feed = payload.resumed
        ? mergeFeed(state.feed, payload.feed)
        : mergeFeed([], payload.feed);
      return {
        ...state,
        status: action.polled ? 'polling' : 'live',
        error: undefined,
        stock: nextStock,
        pulses,
        pulseCounter,
        feed,
        orderTick: latestOrderSeq(feed, payload.resumed ? state.orderTick : 0),
      };
    }

    case 'stock': {
      const known = state.stock?.[action.view.sku];
      if (known && action.view.version < known.version) return state; // older than what we show
      const pulseCounter = state.pulseCounter + 1;
      return {
        ...state,
        stock: { ...state.stock, [action.view.sku]: action.view },
        pulses: { ...state.pulses, [action.view.sku]: pulseCounter },
        pulseCounter,
      };
    }

    case 'feed': {
      if (state.feed.some((e) => e.seq === action.entry.seq)) return state;
      return {
        ...state,
        feed: mergeFeed(state.feed, [action.entry]),
        orderTick: action.entry.kind === 'order' ? action.entry.seq : state.orderTick,
      };
    }
  }
}
