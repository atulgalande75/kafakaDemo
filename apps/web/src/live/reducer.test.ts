import type { SnapshotPayload } from '@orderflow/stream-types';
import { describe, expect, it } from 'vitest';
import { DEFAULT_UI_FLAGS } from '../flags/defaults';
import { feedEntry, stockView } from '../test/fixtures';
import { MAX_FEED, initialLiveState, liveReducer, type LiveState } from './reducer';

const snapshot = (state: LiveState, payload: Partial<SnapshotPayload> = {}) =>
  liveReducer(state, {
    type: 'snapshot',
    payload: { stock: [], feed: [], resumed: false, serverTime: 'now', ...payload },
  });

describe('snapshot', () => {
  it('replaces stock and marks the stream live', () => {
    const state = snapshot(initialLiveState, { stock: [stockView(), stockView({ sku: 'SKU-B' })] });
    expect(state.status).toBe('live');
    expect(Object.keys(state.stock ?? {})).toEqual(['SKU-A', 'SKU-B']);
  });

  it('keeps stock null for a user who may not see it', () => {
    expect(snapshot(initialLiveState, { stock: null }).stock).toBeNull();
  });

  it('shows the feed newest first', () => {
    const state = snapshot(initialLiveState, {
      feed: [feedEntry({ seq: 1 }), feedEntry({ seq: 3 }), feedEntry({ seq: 2 })],
    });
    expect(state.feed.map((e) => e.seq)).toEqual([3, 2, 1]);
  });

  it('replaces the feed on a fresh snapshot but merges it on a resumed one', () => {
    const before = snapshot(initialLiveState, {
      feed: [feedEntry({ seq: 1 }), feedEntry({ seq: 2 })],
    });

    const resumed = snapshot(before, {
      resumed: true,
      feed: [feedEntry({ seq: 2 }), feedEntry({ seq: 3 })],
    });
    expect(resumed.feed.map((e) => e.seq)).toEqual([3, 2, 1]); // no duplicate of 2, nothing lost

    const fresh = snapshot(before, { resumed: false, feed: [feedEntry({ seq: 9 })] });
    expect(fresh.feed.map((e) => e.seq)).toEqual([9]);
  });
});

describe('stock updates', () => {
  const loaded = () =>
    snapshot(initialLiveState, { stock: [stockView({ version: 2, available: 40 })] });

  it('applies a newer level and pulses the row', () => {
    const state = liveReducer(loaded(), {
      type: 'stock',
      view: stockView({ version: 3, available: 35 }),
    });
    expect(state.stock?.['SKU-A']?.available).toBe(35);
    expect(state.pulses['SKU-A']).toBeGreaterThan(0);
  });

  it('ignores an older version', () => {
    const before = loaded();
    const state = liveReducer(before, {
      type: 'stock',
      view: stockView({ version: 1, available: 99 }),
    });
    expect(state).toBe(before);
  });

  it('adds a SKU it has not seen', () => {
    const state = liveReducer(loaded(), { type: 'stock', view: stockView({ sku: 'SKU-NEW' }) });
    expect(Object.keys(state.stock ?? {})).toContain('SKU-NEW');
  });

  it('pulses grow with each update, even on the same SKU', () => {
    let state = loaded();
    state = liveReducer(state, { type: 'stock', view: stockView({ version: 3 }) });
    const first = state.pulses['SKU-A'] ?? 0;
    state = liveReducer(state, { type: 'stock', view: stockView({ version: 4 }) });
    expect(state.pulses['SKU-A']).toBeGreaterThan(first);
  });
});

describe('feed entries', () => {
  it('prepends new entries and ignores one it already has', () => {
    let state = snapshot(initialLiveState, { feed: [feedEntry({ seq: 1 })] });
    state = liveReducer(state, { type: 'feed', entry: feedEntry({ seq: 2 }) });
    const again = liveReducer(state, { type: 'feed', entry: feedEntry({ seq: 2 }) });
    expect(state.feed.map((e) => e.seq)).toEqual([2, 1]);
    expect(again).toBe(state);
  });

  it('moves orderTick only for order events', () => {
    let state = liveReducer(initialLiveState, { type: 'feed', entry: feedEntry({ seq: 4 }) });
    expect(state.orderTick).toBe(4);
    state = liveReducer(state, {
      type: 'feed',
      entry: feedEntry({ seq: 5, kind: 'stock-alert', type: 'stock.low' }),
    });
    expect(state.orderTick).toBe(4);
  });

  it('keeps the feed bounded', () => {
    let state = initialLiveState;
    for (let seq = 1; seq <= MAX_FEED + 25; seq++) {
      state = liveReducer(state, { type: 'feed', entry: feedEntry({ seq }) });
    }
    expect(state.feed).toHaveLength(MAX_FEED);
    expect(state.feed[0]?.seq).toBe(MAX_FEED + 25);
  });
});

describe('status', () => {
  it('records why the stream stopped', () => {
    const state = liveReducer(initialLiveState, { type: 'status', status: 'error', error: 'nope' });
    expect(state).toMatchObject({ status: 'error', error: 'nope' });
  });
});

describe('flags', () => {
  it('starts with the safe defaults, not yet loaded', () => {
    expect(initialLiveState.flags).toEqual(DEFAULT_UI_FLAGS);
    expect(initialLiveState.flagsLoaded).toBe(false);
  });

  it('stores the flags and marks them loaded', () => {
    const flags = { ...DEFAULT_UI_FLAGS, newInventoryDashboard: true };
    const state = liveReducer(initialLiveState, { type: 'flags', flags });
    expect(state).toMatchObject({ flags, flagsLoaded: true });
  });

  it('returns the same state when nothing changed (so polling does not re-render)', () => {
    const loaded = liveReducer(initialLiveState, { type: 'flags', flags: DEFAULT_UI_FLAGS });
    expect(liveReducer(loaded, { type: 'flags', flags: { ...DEFAULT_UI_FLAGS } })).toBe(loaded);
  });
});

describe('polled snapshots', () => {
  it('report polling instead of live', () => {
    const state = liveReducer(initialLiveState, {
      type: 'snapshot',
      payload: { stock: [], feed: [], resumed: false, serverTime: 'x' },
      polled: true,
    });
    expect(state.status).toBe('polling');
  });

  it('light up rows that changed since the last snapshot', () => {
    const first = snapshot(initialLiveState, {
      stock: [stockView({ version: 1 }), stockView({ sku: 'SKU-B', version: 1 })],
    });
    expect(first.pulses).toEqual({}); // the first load highlights nothing

    const second = snapshot(first, {
      stock: [stockView({ version: 3, available: 40 }), stockView({ sku: 'SKU-B', version: 1 })],
    });
    expect(second.pulses['SKU-A']).toBeGreaterThan(0);
    expect(second.pulses['SKU-B']).toBeUndefined();
  });
});
