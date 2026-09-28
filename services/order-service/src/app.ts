import { randomUUID } from 'node:crypto';
import Fastify, { type FastifyInstance } from 'fastify';
import { z } from 'zod';
import { EventTypes, Topics, createEvent, orderItemSchema } from '@orderflow/contracts';
import type { EventProducer, Logger } from '@orderflow/kafka-utils';
import { orderFromEvent } from './order.js';
import type { OrderRepository } from './repository.js';

export const createOrderRequestSchema = z.object({
  customerId: z.string().min(1),
  items: z.array(orderItemSchema).min(1).max(50),
  currency: z.string().length(3).toUpperCase().default('USD'),
});

export interface AppDeps {
  repo: OrderRepository;
  producer: Pick<EventProducer, 'publish'>;
  logger: Logger;
}

const CORRELATION_HEADER = 'x-correlation-id';

export function buildApp({ repo, producer, logger }: AppDeps): FastifyInstance {
  // Fastify's per-request logging is off; we log business events with our own logger.
  const app = Fastify({
    logger: false,
    // Honour an incoming correlation id so a whole flow can be traced across services.
    genReqId: (req) => {
      const header = req.headers[CORRELATION_HEADER];
      return (typeof header === 'string' && header) || randomUUID();
    },
  });

  app.addHook('onSend', async (req, reply) => {
    void reply.header(CORRELATION_HEADER, req.id);
  });

  app.setErrorHandler((err: { statusCode?: number; message: string }, req, reply) => {
    const statusCode = err.statusCode ?? 500;
    if (statusCode >= 500) logger.error({ err, correlationId: req.id }, 'request failed');
    return reply.code(statusCode).send({ error: err.message });
  });

  app.get('/health', () => ({ status: 'ok' }));

  app.post('/orders', async (req, reply) => {
    const parsed = createOrderRequestSchema.safeParse(req.body);
    if (!parsed.success) {
      return reply
        .code(400)
        .send({ error: 'Invalid order', details: z.flattenError(parsed.error).fieldErrors });
    }
    const { customerId, items, currency } = parsed.data;
    const orderId = randomUUID();
    const totalAmount =
      Math.round(items.reduce((sum, i) => sum + i.quantity * i.unitPrice, 0) * 100) / 100;

    const event = createEvent(
      EventTypes.OrderCreated,
      { orderId, customerId, items, totalAmount, currency },
      { correlationId: req.id },
    );
    const order = orderFromEvent(event);

    // Save first, then publish, so outcome events can never arrive for an unknown order.
    // (Production systems use a transactional outbox to make this atomic.)
    await repo.save({ order, createdEvent: event });
    try {
      await producer.publish(Topics.OrdersCreated, event, { key: orderId });
    } catch (err) {
      await repo.delete(orderId);
      logger.error({ err, orderId, correlationId: req.id }, 'failed to publish orders.created');
      return reply.code(503).send({ error: 'Order could not be submitted, try again later' });
    }

    logger.info(
      { orderId, correlationId: req.id, totalAmount, eventId: event.eventId },
      'order created -> orders.created',
    );
    return reply.code(202).header('location', `/orders/${orderId}`).send(order);
  });

  app.get('/orders', async (req) => {
    const { limit } = z
      .object({ limit: z.coerce.number().int().min(1).max(500).default(20) })
      .parse(req.query);
    return repo.list(limit);
  });

  app.get<{ Params: { id: string } }>('/orders/:id', async (req, reply) => {
    const stored = await repo.get(req.params.id);
    if (!stored) return reply.code(404).send({ error: 'Order not found' });
    return stored.order;
  });

  /**
   * DEMO ONLY: publishes the order's original orders.created event again, with the
   * same eventId - exactly what a producer retry after a lost ack looks like.
   * Downstream consumers must detect and skip it.
   */
  app.post<{ Params: { id: string } }>('/orders/:id/republish', async (req, reply) => {
    const stored = await repo.get(req.params.id);
    if (!stored) return reply.code(404).send({ error: 'Order not found' });
    const event = stored.createdEvent;
    await producer.publish(Topics.OrdersCreated, event, { key: event.data.orderId });
    logger.warn(
      { orderId: event.data.orderId, eventId: event.eventId, correlationId: event.correlationId },
      're-published orders.created (duplicate delivery demo)',
    );
    return reply.code(202).send({ republished: true, eventId: event.eventId });
  });

  return app;
}
