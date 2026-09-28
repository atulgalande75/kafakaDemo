import { z } from 'zod';
import { defineEnvelope } from './envelope.js';
import { Topics, type Topic } from './topics.js';

export const EventTypes = {
  OrderCreated: 'order.created',
  PaymentCompleted: 'payment.completed',
  PaymentFailed: 'payment.failed',
  InventoryReserved: 'inventory.reserved',
  InventoryRejected: 'inventory.rejected',
} as const;

export type EventType = (typeof EventTypes)[keyof typeof EventTypes];

const money = z.number().nonnegative().finite();
const currency = z.string().length(3).toUpperCase();

export const orderItemSchema = z.object({
  sku: z.string().min(1),
  quantity: z.number().int().positive(),
  unitPrice: money,
});

export const CUSTOMER_TIERS = ['standard', 'gold', 'platinum'] as const;
export const customerTierSchema = z.enum(CUSTOMER_TIERS);
export type CustomerTier = z.infer<typeof customerTierSchema>;

/** ISO 3166-1 alpha-2 country code, e.g. "US". */
export const countrySchema = z
  .string()
  .regex(/^[A-Za-z]{2}$/, 'must be a 2-letter country code')
  .toUpperCase();

export const orderCreatedDataSchema = z.object({
  orderId: z.uuid(),
  customerId: z.string().min(1),
  // Added later with defaults, so version 1 events without them remain valid.
  customerTier: customerTierSchema.default('standard'),
  country: countrySchema.default('US'),
  items: z.array(orderItemSchema).min(1),
  totalAmount: money,
  currency,
});

export const paymentCompletedDataSchema = z.object({
  orderId: z.uuid(),
  paymentId: z.uuid(),
  amount: money,
  currency,
});

export const paymentFailedDataSchema = z.object({
  orderId: z.uuid(),
  reason: z.string().min(1),
});

export const inventoryReservedDataSchema = z.object({
  orderId: z.uuid(),
  reservationId: z.uuid(),
  items: z.array(z.object({ sku: z.string().min(1), quantity: z.number().int().positive() })),
});

export const inventoryRejectedDataSchema = z.object({
  orderId: z.uuid(),
  reason: z.string().min(1),
  unavailable: z.array(
    z.object({
      sku: z.string().min(1),
      requested: z.number().int().positive(),
      available: z.number().int().nonnegative(),
    }),
  ),
});

/** Current (latest) schema for each event type. */
export const eventSchemas = {
  [EventTypes.OrderCreated]: defineEnvelope(EventTypes.OrderCreated, 1, orderCreatedDataSchema),
  [EventTypes.PaymentCompleted]: defineEnvelope(
    EventTypes.PaymentCompleted,
    1,
    paymentCompletedDataSchema,
  ),
  [EventTypes.PaymentFailed]: defineEnvelope(EventTypes.PaymentFailed, 1, paymentFailedDataSchema),
  [EventTypes.InventoryReserved]: defineEnvelope(
    EventTypes.InventoryReserved,
    1,
    inventoryReservedDataSchema,
  ),
  [EventTypes.InventoryRejected]: defineEnvelope(
    EventTypes.InventoryRejected,
    1,
    inventoryRejectedDataSchema,
  ),
} as const;

export type EventOf<T extends EventType> = z.infer<(typeof eventSchemas)[T]>;
export type DataOf<T extends EventType> = EventOf<T>['data'];
/** What producers pass in: like {@link DataOf}, but fields with defaults are optional. */
export type DataInputOf<T extends EventType> = z.input<(typeof eventSchemas)[T]>['data'];

export type OrderCreatedEvent = EventOf<'order.created'>;
export type PaymentCompletedEvent = EventOf<'payment.completed'>;
export type PaymentFailedEvent = EventOf<'payment.failed'>;
export type InventoryReservedEvent = EventOf<'inventory.reserved'>;
export type InventoryRejectedEvent = EventOf<'inventory.rejected'>;
export type OrderItem = z.infer<typeof orderItemSchema>;

export type AnyEvent = { [T in EventType]: EventOf<T> }[EventType];

/** Which event type is published on which topic (one type per topic). */
export const topicEventType = {
  [Topics.OrdersCreated]: EventTypes.OrderCreated,
  [Topics.PaymentsCompleted]: EventTypes.PaymentCompleted,
  [Topics.PaymentsFailed]: EventTypes.PaymentFailed,
  [Topics.InventoryReserved]: EventTypes.InventoryReserved,
  [Topics.InventoryRejected]: EventTypes.InventoryRejected,
} as const satisfies Record<Topic, EventType>;

export type EventForTopic<T extends Topic> = EventOf<(typeof topicEventType)[T]>;

export const eventTypeTopic = Object.fromEntries(
  Object.entries(topicEventType).map(([topic, type]) => [type, topic]),
) as { [T in EventType]: Topic };
