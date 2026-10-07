import type { FetchEventSourceInit } from '@microsoft/fetch-event-source';
import type { SnapshotResponse, UiFlags } from '@orderflow/stream-types';
import { act, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type * as ClientModule from '../api/client';
import { ApiError } from '../api/client';
import { DEFAULT_UI_FLAGS } from '../flags/defaults';
import { feedEntry, stockView } from '../test/fixtures';
import { LiveProvider, POLL_MS, STALE_MS, WATCHDOG_MS, useLive } from './LiveProvider';

const hoisted = vi.hoisted(() => ({
  options: undefined as FetchEventSourceInit | undefined,
  opened: 0,
  getToken: undefined as (() => Promise<string>) | undefined,
  api: undefined as ((...args: unknown[]) => Promise<unknown>) | undefined,
}));

vi.mock('@microsoft/fetch-event-source', () => ({
  EventStreamContentType: 'text/event-stream',
  fetchEventSource: vi.fn((_url: string, options: FetchEventSourceInit) => {
    hoisted.options = options;
    hoisted.opened++;
    return new Promise<void>(() => {}); // stays "connected" until the test ends
  }),
}));
vi.mock('../auth/AuthProvider', () => ({
  useAuth: () => ({ getAccessToken: hoisted.getToken }),
}));
vi.mock('../api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof ClientModule>()),
  api: (...args: unknown[]) => hoisted.api!(...args),
}));

const options = () => hoisted.options!;
const sse = (status = 200, contentType = 'text/event-stream') =>
  new Response('', { status, headers: { 'content-type': contentType } });
const frame = (event: string, data: unknown) => ({
  id: '',
  event,
  data: JSON.stringify(data),
  retry: undefined,
});
const flags = (over: Partial<UiFlags> = {}): UiFlags => ({ ...DEFAULT_UI_FLAGS, ...over });
const snapshotResponse = (over: Partial<SnapshotResponse> = {}): SnapshotResponse => ({
  stock: [stockView()],
  feed: [feedEntry({ seq: 1 })],
  resumed: false,
  serverTime: 'x',
  flags: flags({ liveUpdates: false }),
  ...over,
});

function Probe() {
  const live = useLive();
  return (
    <div>
      <span data-testid="status">{live.status}</span>
      <span data-testid="error">{live.error}</span>
      <span data-testid="skus">{Object.keys(live.stock ?? {}).join(',')}</span>
      <span data-testid="feed">{live.feed.map((e) => e.seq).join(',')}</span>
      <span data-testid="dashboard">{String(live.flags.newInventoryDashboard)}</span>
    </div>
  );
}
const text = (id: string) => screen.getByTestId(id).textContent;

/** Renders the provider and waits for the flags answer. */
async function mount() {
  render(
    <LiveProvider>
      <Probe />
    </LiveProvider>,
  );
  await act(async () => {
    await vi.advanceTimersByTimeAsync(0);
  });
}

beforeEach(() => {
  vi.useFakeTimers();
  hoisted.options = undefined;
  hoisted.opened = 0;
  hoisted.getToken = vi.fn().mockResolvedValue('token-1');
  hoisted.api = vi.fn((_getToken, _service, path) =>
    Promise.resolve(path === '/flags' ? flags() : snapshotResponse()),
  );
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('streaming (live-updates-enabled on)', () => {
  it('asks for the flags before opening the stream', async () => {
    render(
      <LiveProvider>
        <Probe />
      </LiveProvider>,
    );
    expect(hoisted.opened).toBe(0);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(hoisted.opened).toBe(1);
    expect(hoisted.api).toHaveBeenCalledWith(
      hoisted.getToken,
      'gateway-service',
      '/flags',
      expect.anything(),
    );
  });

  it('goes live on the snapshot', async () => {
    await mount();
    expect(text('status')).toBe('connecting');
    await act(async () => {
      await options().onopen?.(sse());
      options().onmessage?.(
        frame('snapshot', {
          stock: [stockView()],
          feed: [feedEntry({ seq: 1 })],
          resumed: false,
          serverTime: 'x',
        }),
      );
    });
    expect(text('status')).toBe('live');
    expect(text('skus')).toBe('SKU-A');
    expect(text('feed')).toBe('1');
  });

  it('applies stock, feed and flags frames, and ignores junk', async () => {
    await mount();
    act(() => {
      options().onmessage?.(
        frame('snapshot', { stock: [stockView()], feed: [], resumed: false, serverTime: 'x' }),
      );
      options().onmessage?.(frame('stock', stockView({ sku: 'SKU-B', version: 1 })));
      options().onmessage?.(frame('feed', feedEntry({ seq: 7 })));
      options().onmessage?.(frame('flags', flags({ newInventoryDashboard: true })));
      options().onmessage?.({ id: '', event: 'feed', data: 'not json', retry: undefined });
      options().onmessage?.(frame('unknown', {}));
    });
    expect(text('skus')).toBe('SKU-A,SKU-B');
    expect(text('feed')).toBe('7');
    expect(text('dashboard')).toBe('true');
  });

  it('sends a fresh bearer token on every (re)connect', async () => {
    await mount();
    const fetchMock = vi.fn().mockResolvedValue(sse());
    vi.stubGlobal('fetch', fetchMock);
    await options().fetch!('/api/gateway-service/stream', { headers: { 'last-event-id': 'e:3' } });
    const init = fetchMock.mock.calls[0]![1] as RequestInit;
    expect(init.headers).toMatchObject({
      authorization: 'Bearer token-1',
      'last-event-id': 'e:3',
    });
  });

  it('reconnects when the server closes the stream or the network fails', async () => {
    await mount();
    expect(() => options().onclose?.()).toThrow();
    let retryIn: unknown;
    act(() => {
      retryIn = options().onerror?.(new Error('network'));
    });
    expect(retryIn).toBe(2000);
    expect(text('status')).toBe('reconnecting');
  });

  it('stops for good when the server refuses the token', async () => {
    await mount();
    const refusal = new Response(JSON.stringify({ message: 'Insufficient scope' }), {
      status: 403,
    });
    const failure = await options()
      .onopen?.(refusal)
      .catch((e: unknown) => e);
    expect(failure).toBeInstanceOf(Error);
    act(() => {
      expect(() => options().onerror?.(failure)).toThrow('Insufficient scope');
    });
    expect(text('status')).toBe('error');
    expect(text('error')).toBe('Insufficient scope');
  });

  it('treats a server error as temporary', async () => {
    await mount();
    const failure = await options()
      .onopen?.(sse(503, 'text/plain'))
      .catch((e: unknown) => e);
    act(() => {
      expect(options().onerror?.(failure)).toBe(2000);
    });
    expect(text('status')).toBe('reconnecting');
  });
});

describe('a connection that goes quiet', () => {
  const advance = (ms: number) =>
    act(async () => {
      await vi.advanceTimersByTimeAsync(ms);
    });

  it('is replaced after three missed heartbeats', async () => {
    await mount();
    const first = options();
    expect(hoisted.opened).toBe(1);

    await advance(STALE_MS + WATCHDOG_MS * 2);
    expect(hoisted.opened).toBe(2);
    expect(first.signal?.aborted).toBe(true); // the dead one is abandoned
    expect(text('status')).toBe('connecting');
  });

  it('stays connected while heartbeats keep arriving', async () => {
    await mount();
    for (let i = 0; i < 8; i++) {
      await advance(15_000);
      act(() => {
        options().onmessage?.(frame('ping', {}));
      });
    }
    expect(hoisted.opened).toBe(1); // 2 minutes with a ping every 15 s
  });

  it('resumes from the last event id it saw', async () => {
    await mount();
    act(() => {
      options().onmessage?.({
        ...frame('feed', feedEntry({ seq: 4 })),
        id: 'epoch:4',
      });
    });
    await advance(STALE_MS + WATCHDOG_MS * 2);
    expect(hoisted.opened).toBe(2);
    expect(options().headers).toMatchObject({ 'last-event-id': 'epoch:4' });
  });

  it('starts without a Last-Event-ID the first time', async () => {
    await mount();
    expect(options().headers).toEqual({});
  });
});

describe('polling (live-updates-enabled off)', () => {
  beforeEach(() => {
    hoisted.api = vi.fn((_getToken, _service, path) =>
      Promise.resolve(path === '/flags' ? flags({ liveUpdates: false }) : snapshotResponse()),
    );
  });
  const snapshotCalls = () =>
    (hoisted.api as ReturnType<typeof vi.fn>).mock.calls.filter((c) => c[2] === '/snapshot');

  it('never opens the stream, and loads a snapshot straight away', async () => {
    await mount();
    expect(hoisted.opened).toBe(0);
    expect(text('status')).toBe('polling');
    expect(text('skus')).toBe('SKU-A');
    expect(text('feed')).toBe('1');
  });

  it('refreshes every few seconds', async () => {
    await mount();
    expect(snapshotCalls()).toHaveLength(1);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(POLL_MS * 2);
    });
    expect(snapshotCalls()).toHaveLength(3);
  });

  it('switches to streaming when a poll says the flag is back on', async () => {
    await mount();
    hoisted.api = vi.fn((_g, _s, path) =>
      Promise.resolve(
        path === '/flags' ? flags() : snapshotResponse({ flags: flags({ liveUpdates: true }) }),
      ),
    );
    await act(async () => {
      await vi.advanceTimersByTimeAsync(POLL_MS);
    });
    expect(hoisted.opened).toBe(1);
    expect(text('status')).toBe('connecting');
    // ...and polling has stopped
    const before = snapshotCalls().length;
    await act(async () => {
      await vi.advanceTimersByTimeAsync(POLL_MS * 3);
    });
    expect(snapshotCalls().length).toBe(before);
  });

  it('keeps retrying after a temporary failure but stops when the server says no', async () => {
    await mount();
    hoisted.api = vi.fn(() => Promise.reject(new Error('network down')));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(POLL_MS);
    });
    expect(text('status')).toBe('reconnecting');

    hoisted.api = vi.fn(() => Promise.reject(new ApiError(403, 'Not allowed')));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(POLL_MS);
    });
    expect(text('status')).toBe('error');
    expect(text('error')).toBe('Not allowed');
    const calls = (hoisted.api as ReturnType<typeof vi.fn>).mock.calls.length;
    await act(async () => {
      await vi.advanceTimersByTimeAsync(POLL_MS * 3);
    });
    expect((hoisted.api as ReturnType<typeof vi.fn>).mock.calls.length).toBe(calls);
  });
});

describe('switching while connected', () => {
  it('moves from streaming to polling when a flags frame turns live updates off', async () => {
    await mount();
    expect(hoisted.opened).toBe(1);
    hoisted.api = vi.fn((_g, _s, path) =>
      Promise.resolve(path === '/flags' ? flags({ liveUpdates: false }) : snapshotResponse()),
    );
    await act(async () => {
      options().onmessage?.(frame('flags', flags({ liveUpdates: false })));
      await vi.advanceTimersByTimeAsync(0);
    });
    expect(text('status')).toBe('polling');
    expect(
      (hoisted.api as ReturnType<typeof vi.fn>).mock.calls.some((c) => c[2] === '/snapshot'),
    ).toBe(true);
  });
});

describe('when the gateway cannot be asked for flags', () => {
  it('falls back to the safe defaults and streams', async () => {
    hoisted.api = vi.fn(() => Promise.reject(new Error('gateway down')));
    await mount();
    expect(hoisted.opened).toBe(1);
    expect(text('dashboard')).toBe('false');
  });
});
