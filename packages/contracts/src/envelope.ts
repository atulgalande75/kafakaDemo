import { z } from 'zod';

/**
 * Every event on the wire is wrapped in the same envelope.
 *
 * - eventId:       unique per event; consumers use it for idempotency
 * - type:          e.g. "order.created"
 * - version:       schema version of `data` for this type
 * - occurredAt:    ISO-8601 timestamp of when the fact happened
 * - correlationId: shared by every event caused by the same original request
 * - actor:         who started the flow (OAuth subject + client). Identity only -
 *                  access tokens are never put in events.
 * - data:          type-specific payload
 */
export const actorSchema = z.strictObject({
  sub: z.string().min(1),
  clientId: z.string().min(1),
});

export type Actor = z.infer<typeof actorSchema>;

export const envelopeSchema = z.object({
  eventId: z.uuid(),
  type: z.string().min(1),
  version: z.number().int().positive(),
  occurredAt: z.iso.datetime({ offset: true }),
  correlationId: z.string().min(1),
  // Optional so events produced before authentication existed still decode.
  actor: actorSchema.optional(),
  data: z.unknown(),
});

export type Envelope<TType extends string = string, TData = unknown> = Omit<
  z.infer<typeof envelopeSchema>,
  'type' | 'data'
> & {
  type: TType;
  data: TData;
};

/** Builds a strict envelope schema for one event type + version. */
export function defineEnvelope<
  TType extends string,
  TVersion extends number,
  TData extends z.ZodType,
>(type: TType, version: TVersion, data: TData) {
  return envelopeSchema.extend({
    type: z.literal(type),
    version: z.literal(version),
    data,
  });
}
