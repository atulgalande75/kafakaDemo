import { EventStreamContentType, fetchEventSource } from '@microsoft/fetch-event-source';
import type {
  FeedEntry,
  SnapshotPayload,
  SnapshotResponse,
  StockView,
  UiFlags,
} from '@orderflow/stream-types';
import {
  createContext,
  useContext,
  useEffect,
  useReducer,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import { ApiError, api } from '../api/client';
import { useAuth } from '../auth/AuthProvider';
import { config } from '../config';
import { DEFAULT_UI_FLAGS } from '../flags/defaults';
import { initialLiveState, liveReducer, type LiveState } from './reducer';

const LiveContext = createContext<LiveState | undefined>(undefined);

export function useLive(): LiveState {
  const value = useContext(LiveContext);
  if (!value) throw new Error('useLive must be used inside <LiveProvider>');
  return value;
}

/** A failure that is not worth retrying (the server said no). */
class FatalStreamError extends Error {}
/** The connection ended or failed; the library reconnects (and sends Last-Event-ID). */
class RetriableStreamError extends Error {}

export const STREAM_URL = `${config.apiBase['gateway-service']}/stream`;
/** How often the app asks for a snapshot while the live-updates flag is off. */
export const POLL_MS = 5000;
/**
 * The gateway sends a heartbeat every 15 s. If nothing at all arrives for this long the
 * connection is dead even though the browser hasn't noticed (a proxy or a network hop left it
 * open), so the app reconnects. Three missed heartbeats is a safe margin.
 */
export const STALE_MS = 45_000;
export const WATCHDOG_MS = 5000;

/**
 * Brings live data into the app, and decides *how* from the user's feature flags:
 *
 * 1. Ask the gateway for the flags (`GET /flags`); on any failure use the safe defaults.
 * 2. `live-updates-enabled` on  -> one SSE connection. It reconnects on its own, sends a fresh
 *    token each time and resumes from `Last-Event-ID`. Flag changes arrive as `flags` frames.
 * 3. `live-updates-enabled` off -> poll `GET /snapshot` every few seconds (it carries the flags too).
 *
 * Flipping the flag switches transport without a reload, in either direction.
 */
export function LiveProvider({ children }: { children: ReactNode }) {
  const { getAccessToken } = useAuth();
  const [state, dispatch] = useReducer(liveReducer, initialLiveState);
  // Bumped by the watchdog to start a fresh connection; the last id lets it resume.
  const [connection, setConnection] = useState(0);
  const lastEventId = useRef<string | undefined>(undefined);

  // 1. Flags first: nothing is opened before we know which transport to use.
  useEffect(() => {
    const controller = new AbortController();
    api<UiFlags>(getAccessToken, 'gateway-service', '/flags', { signal: controller.signal })
      .then((flags) => dispatch({ type: 'flags', flags }))
      .catch(() => {
        if (!controller.signal.aborted) dispatch({ type: 'flags', flags: DEFAULT_UI_FLAGS });
      });
    return () => controller.abort();
  }, [getAccessToken]);

  const mode = !state.flagsLoaded ? 'pending' : state.flags.liveUpdates ? 'stream' : 'poll';

  // 2. Streaming.
  useEffect(() => {
    if (mode !== 'stream') return;
    const controller = new AbortController();
    dispatch({ type: 'status', status: 'connecting' });

    let lastHeard = Date.now();
    const watchdog = setInterval(() => {
      if (Date.now() - lastHeard <= STALE_MS) return;
      dispatch({ type: 'status', status: 'reconnecting' });
      setConnection((n) => n + 1); // re-runs this effect: the old connection is aborted
    }, WATCHDOG_MS);

    void fetchEventSource(STREAM_URL, {
      signal: controller.signal,
      headers: lastEventId.current ? { 'last-event-id': lastEventId.current } : {},
      openWhenHidden: true, // keep streaming in a background tab
      // A fresh token on every (re)connect: access tokens are short-lived.
      fetch: async (input, init) =>
        window.fetch(input, {
          ...init,
          headers: { ...init?.headers, authorization: `Bearer ${await getAccessToken()}` },
        }),

      async onopen(response) {
        if (
          response.ok &&
          response.headers.get('content-type')?.startsWith(EventStreamContentType)
        ) {
          return; // the snapshot frame that follows marks us live
        }
        if (response.status >= 400 && response.status < 500 && response.status !== 429) {
          throw new FatalStreamError(await explain(response));
        }
        throw new RetriableStreamError(`Unexpected response (${response.status})`);
      },

      onmessage(message) {
        lastHeard = Date.now();
        if (message.id) lastEventId.current = message.id;
        let data: unknown;
        try {
          data = JSON.parse(message.data);
        } catch {
          return;
        }
        switch (message.event) {
          case 'snapshot':
            dispatch({ type: 'snapshot', payload: data as SnapshotPayload });
            break;
          case 'stock':
            dispatch({ type: 'stock', view: data as StockView });
            break;
          case 'feed':
            dispatch({ type: 'feed', entry: data as FeedEntry });
            break;
          case 'flags':
            dispatch({ type: 'flags', flags: data as UiFlags });
            break;
          // 'ping' only proves the connection is alive (lastHeard is already updated)
        }
      },

      onclose() {
        // The server ended the stream (restart, slow-client drop): reconnect and resume.
        throw new RetriableStreamError('Stream closed');
      },

      onerror(err) {
        if (err instanceof FatalStreamError) {
          dispatch({ type: 'status', status: 'error', error: err.message });
          throw err; // stop retrying
        }
        dispatch({ type: 'status', status: 'reconnecting' });
        return 2000; // retry in 2 s
      },
    }).catch(() => {
      /* a fatal error was already reported through the status */
    });

    return () => {
      clearInterval(watchdog);
      controller.abort();
    };
  }, [mode, getAccessToken, connection]);

  // 3. Polling.
  useEffect(() => {
    if (mode !== 'poll') return;
    const controller = new AbortController();
    dispatch({ type: 'status', status: 'connecting' });

    const poll = async () => {
      try {
        const { flags, ...payload } = await api<SnapshotResponse>(
          getAccessToken,
          'gateway-service',
          '/snapshot',
          { signal: controller.signal },
        );
        dispatch({ type: 'snapshot', payload, polled: true });
        dispatch({ type: 'flags', flags }); // may switch us back to streaming
      } catch (err) {
        if (controller.signal.aborted) return;
        if (err instanceof ApiError && (err.status === 401 || err.status === 403)) {
          dispatch({ type: 'status', status: 'error', error: err.message });
          clearInterval(timer); // the server said no: asking again won't help
          return;
        }
        dispatch({ type: 'status', status: 'reconnecting' });
      }
    };

    const timer = setInterval(() => void poll(), POLL_MS);
    void poll();
    return () => {
      controller.abort();
      clearInterval(timer);
    };
  }, [mode, getAccessToken]);

  return <LiveContext.Provider value={state}>{children}</LiveContext.Provider>;
}

async function explain(response: Response): Promise<string> {
  try {
    const body = (await response.json()) as { message?: string; error?: string };
    return body.message ?? body.error ?? `HTTP ${response.status}`;
  } catch {
    return `HTTP ${response.status}`;
  }
}
