import { randomUUID } from 'node:crypto';
import type { OutgoingHttpHeaders } from 'node:http';
import cors from '@fastify/cors';
import Fastify, { type FastifyInstance } from 'fastify';
import {
  AuthError,
  Scopes,
  authPlugin,
  requireScope,
  sendAuthError,
  type AuthContext,
  type TokenVerifier,
} from '@orderflow/auth';
import { isUiFlagKey, type FeatureFlags } from '@orderflow/feature-flags';
import type { Logger } from '@orderflow/kafka-utils';
import { canSeeOrders, canSeeStock, type Hub, type Principal } from './hub.js';
import { HEARTBEAT, formatFrame } from './sse.js';
import { evaluateUiFlags, sameUiFlags, type FlagUser } from './ui-flags.js';

export interface AppDeps {
  hub: Hub;
  /** Source of the web app's feature flags (LaunchDarkly or the local file). */
  flags: FeatureFlags;
  logger: Logger;
  verifier: TokenVerifier;
  /** Browser origins allowed to call the gateway (the React dev server by default). */
  allowedOrigins?: string[];
  heartbeatMs?: number;
  /** A client that falls this far behind (bytes queued on its socket) is dropped; it reconnects and resumes. */
  maxBufferedBytes?: number;
}

export function buildApp({
  hub,
  flags,
  logger,
  verifier,
  allowedOrigins = ['http://localhost:5173'],
  heartbeatMs = 15_000,
  maxBufferedBytes = 1_000_000,
}: AppDeps): FastifyInstance {
  const app = Fastify({ logger: false, genReqId: () => randomUUID() });

  // Open streams are hijacked, so Fastify can't see them. End them as soon as shutdown starts
  // (`preClose` runs before the server stops waiting for connections; `onClose` runs after it
  // has finished, which would never happen), otherwise clients only find out when the
  // process dies.
  const openStreams = new Set<() => void>();
  app.addHook('preClose', () => {
    for (const close of [...openStreams]) close();
  });

  void app.register(cors, {
    origin: allowedOrigins,
    allowedHeaders: ['authorization', 'content-type', 'last-event-id', 'x-correlation-id'],
    exposedHeaders: ['x-correlation-id'],
  });
  void app.register(authPlugin, { verifier, realm: 'orderflow-gateway' });

  app.setErrorHandler((err: { statusCode?: number; message: string }, req, reply) => {
    if (err instanceof AuthError) return sendAuthError(reply, err);
    const statusCode = err.statusCode ?? 500;
    if (statusCode >= 500) logger.error({ err, requestId: req.id }, 'request failed');
    return reply.code(statusCode).send({ error: err.message });
  });

  app.get('/health', () => ({
    status: 'ok',
    clients: hub.clients,
    skus: hub.skuCount,
    feedPosition: hub.lastSeq,
  }));

  const mayStream = requireScope(Scopes.StreamRead);

  /** Rejects tokens that may connect but have no data scope: they'd get an empty stream. */
  const principalOf = (req: { auth: { sub: string; scopes: ReadonlySet<string> } | null }) => {
    const principal: Principal = { sub: req.auth!.sub, scopes: req.auth!.scopes };
    if (!canSeeStock(principal) && !canSeeOrders(principal)) {
      throw AuthError.insufficientScope([Scopes.OrdersRead, Scopes.InventoryRead]);
    }
    return principal;
  };

  const flagUserOf = (req: { auth: AuthContext | null }): FlagUser => {
    const username = req.auth!.claims.preferred_username;
    return { sub: req.auth!.sub, ...(typeof username === 'string' && { username }) };
  };

  /** The web app's feature flags, evaluated for the signed-in user. */
  app.get('/flags', { preHandler: mayStream }, (req) => evaluateUiFlags(flags, flagUserOf(req)));

  /** The current state as plain JSON (same payload as the stream's first frame). */
  app.get('/snapshot', { preHandler: mayStream }, async (req) => {
    const principal = principalOf(req);
    return { ...hub.snapshot(principal), flags: await evaluateUiFlags(flags, flagUserOf(req)) };
  });

  /**
   * Server-Sent Events: one `snapshot` frame, then `stock` and `feed` frames as they happen.
   * Send `Last-Event-ID` to resume after a dropped connection.
   */
  app.get('/stream', { preHandler: mayStream }, async (req, reply) => {
    const principal = principalOf(req);
    const flagUser = flagUserOf(req);
    const lastEventId = req.headers['last-event-id'];
    let currentFlags = await evaluateUiFlags(flags, flagUser);

    reply.hijack();
    const res = reply.raw;
    res.writeHead(200, {
      ...(reply.getHeaders() as OutgoingHttpHeaders), // CORS headers set by the plugin
      'content-type': 'text/event-stream; charset=utf-8',
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'x-accel-buffering': 'no', // don't let nginx buffer the stream
    });

    let closed = false;
    let disconnect = () => {};
    let unwatchFlags = () => {};
    const close = () => {
      if (closed) return;
      closed = true;
      clearInterval(heartbeat);
      openStreams.delete(close);
      unwatchFlags();
      disconnect();
      res.end();
      logger.info({ sub: principal.sub, clients: hub.clients }, 'stream closed');
    };
    const write = (chunk: string) => {
      if (closed) return;
      res.write(chunk);
      if (res.writableLength > maxBufferedBytes) {
        logger.warn({ sub: principal.sub }, 'slow stream client dropped');
        close();
      }
    };

    openStreams.add(close);
    const heartbeat = setInterval(() => write(HEARTBEAT), heartbeatMs);
    req.raw.on('close', close);
    disconnect = hub.connect(
      principal,
      (frame) => write(formatFrame(frame)),
      typeof lastEventId === 'string' ? lastEventId : undefined,
    );
    write(formatFrame({ event: 'flags', data: currentFlags }));
    // Flag changes reach open streams right away; the user's context is evaluated again each time.
    unwatchFlags = flags.onChange((key) => {
      if (!isUiFlagKey(key)) return;
      void evaluateUiFlags(flags, flagUser).then((next) => {
        if (closed || sameUiFlags(next, currentFlags)) return;
        currentFlags = next;
        write(formatFrame({ event: 'flags', data: next }));
      });
    });
    logger.info(
      { sub: principal.sub, resumed: Boolean(lastEventId), clients: hub.clients },
      'stream opened',
    );
    return reply;
  });

  return app;
}
