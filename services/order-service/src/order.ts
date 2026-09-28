import type {
  Actor,
  CustomerTier,
  InventoryRejectedEvent,
  InventoryReservedEvent,
  OrderCreatedEvent,
  OrderItem,
  PaymentCompletedEvent,
  PaymentFailedEvent,
} from '@orderflow/contracts';

export type OrderStatus = 'PENDING' | 'CONFIRMED' | 'CANCELLED';

export interface Order {
  id: string;
  customerId: string;
  customerTier: CustomerTier;
  country: string;
  items: OrderItem[];
  totalAmount: number;
  currency: string;
  status: OrderStatus;
  payment: { status: 'PENDING' | 'COMPLETED' | 'FAILED'; paymentId?: string; reason?: string };
  inventory: {
    status: 'PENDING' | 'RESERVED' | 'REJECTED';
    reservationId?: string;
    reason?: string;
  };
  correlationId: string;
  /** Who placed the order (from the access token). Used for ownership checks. */
  createdBy?: Actor;
  createdAt: string;
  updatedAt: string;
  /** Audit trail of the events that changed this order. */
  history: Array<{ at: string; eventType: string; eventId: string; status: OrderStatus }>;
}

export type OutcomeEvent =
  PaymentCompletedEvent | PaymentFailedEvent | InventoryReservedEvent | InventoryRejectedEvent;

export function orderFromEvent(event: OrderCreatedEvent): Order {
  const { orderId, customerId, customerTier, country, items, totalAmount, currency } = event.data;
  return {
    id: orderId,
    customerId,
    customerTier,
    country,
    items,
    totalAmount,
    currency,
    status: 'PENDING',
    payment: { status: 'PENDING' },
    inventory: { status: 'PENDING' },
    correlationId: event.correlationId,
    createdBy: event.actor,
    createdAt: event.occurredAt,
    updatedAt: event.occurredAt,
    history: [
      { at: event.occurredAt, eventType: event.type, eventId: event.eventId, status: 'PENDING' },
    ],
  };
}

/**
 * The order saga: PENDING until both payment and inventory have answered.
 *   payment COMPLETED + inventory RESERVED  -> CONFIRMED
 *   payment FAILED    or inventory REJECTED -> CANCELLED (as soon as either fails)
 * CONFIRMED and CANCELLED are final. Applying the same event twice is a no-op.
 */
export function applyOutcome(order: Order, event: OutcomeEvent): Order {
  if (order.history.some((h) => h.eventId === event.eventId)) return order;

  const next: Order = {
    ...order,
    payment: { ...order.payment },
    inventory: { ...order.inventory },
  };
  switch (event.type) {
    case 'payment.completed':
      next.payment = { status: 'COMPLETED', paymentId: event.data.paymentId };
      break;
    case 'payment.failed':
      next.payment = { status: 'FAILED', reason: event.data.reason };
      break;
    case 'inventory.reserved':
      next.inventory = { status: 'RESERVED', reservationId: event.data.reservationId };
      break;
    case 'inventory.rejected':
      next.inventory = { status: 'REJECTED', reason: event.data.reason };
      break;
  }

  next.status = deriveStatus(order.status, next);
  next.updatedAt = event.occurredAt;
  next.history = [
    ...order.history,
    { at: event.occurredAt, eventType: event.type, eventId: event.eventId, status: next.status },
  ];
  return next;
}

function deriveStatus(current: OrderStatus, order: Order): OrderStatus {
  if (current !== 'PENDING') return current;
  if (order.payment.status === 'FAILED' || order.inventory.status === 'REJECTED') {
    return 'CANCELLED';
  }
  if (order.payment.status === 'COMPLETED' && order.inventory.status === 'RESERVED') {
    return 'CONFIRMED';
  }
  return 'PENDING';
}
