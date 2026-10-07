import { randomUUID } from 'node:crypto';
import Fastify, { type FastifyInstance } from 'fastify';
import { z } from 'zod';
import {
  AuthError,
  Scopes,
  authPlugin,
  requireScope,
  sendAuthError,
  type TokenVerifier,
} from '@orderflow/auth';
import type { Logger } from '@orderflow/kafka-utils';
import { InsufficientStockError, UnknownSkuError, type InventoryStore } from './inventory.js';

export const adjustRequestSchema = z
  .object({
    delta: z
      .number()
      .int()
      .refine((n) => n !== 0, 'must not be 0'),
    reason: z.enum(['restock', 'shrinkage', 'correction']),
    note: z.string().max(500).optional(),
  })
  .refine((v) => v.reason !== 'restock' || v.delta > 0, {
    message: 'a restock must add stock (delta > 0)',
    path: ['delta'],
  })
  .refine((v) => v.reason !== 'shrinkage' || v.delta < 0, {
    message: 'shrinkage must remove stock (delta < 0)',
    path: ['delta'],
  });

export interface AppDeps {
  inventory: InventoryStore;
  /** Wakes the outbox relay after a write. */
  relay: { poke(): void };
  logger: Logger;
  verifier: TokenVerifier;
}

const CORRELATION_HEADER = 'x-correlation-id';

export function buildApp({ inventory, relay, logger, verifier }: AppDeps): FastifyInstance {
  const app = Fastify({
    logger: false,
    genReqId: (req) => {
      const header = req.headers[CORRELATION_HEADER];
      return (typeof header === 'string' && header) || randomUUID();
    },
  });

  void app.register(authPlugin, { verifier, realm: 'orderflow-inventory' });

  app.addHook('onSend', async (req, reply) => {
    void reply.header(CORRELATION_HEADER, req.id);
  });

  app.setErrorHandler((err: { statusCode?: number; message: string }, req, reply) => {
    if (err instanceof AuthError) return sendAuthError(reply, err);
    if (err instanceof UnknownSkuError) return reply.code(404).send({ error: err.message });
    if (err instanceof InsufficientStockError) {
      return reply
        .code(409)
        .send({ error: 'insufficient_stock', message: err.message, available: err.available });
    }
    const statusCode = err.statusCode ?? 500;
    if (statusCode >= 500) logger.error({ err, correlationId: req.id }, 'request failed');
    return reply.code(statusCode).send({ error: err.message });
  });

  app.get('/health', () => ({ status: 'ok' }));

  const canRead = requireScope(Scopes.InventoryRead, Scopes.InventoryWrite);

  app.get('/inventory', { preHandler: canRead }, () => inventory.list());

  app.get<{ Params: { sku: string } }>('/inventory/:sku', { preHandler: canRead }, async (req) => {
    const item = await inventory.get(req.params.sku);
    if (!item) throw new UnknownSkuError(req.params.sku);
    return item;
  });

  app.post<{ Params: { sku: string } }>(
    '/inventory/:sku/adjust',
    { preHandler: requireScope(Scopes.InventoryWrite) },
    async (req, reply) => {
      const parsed = adjustRequestSchema.safeParse(req.body);
      if (!parsed.success) {
        return reply.code(400).send({
          error: 'Invalid adjustment',
          details: z.flattenError(parsed.error).fieldErrors,
        });
      }
      const { delta, reason, note } = parsed.data;
      const actor = { sub: req.auth!.sub, clientId: req.auth!.clientId };
      const item = await inventory.adjust(
        req.params.sku,
        delta,
        reason,
        { correlationId: req.id, actor },
        note,
      );
      relay.poke();
      logger.info(
        { sku: item.sku, delta, reason, available: item.available, correlationId: req.id, actor },
        'stock adjusted',
      );
      return item;
    },
  );

  return app;
}
