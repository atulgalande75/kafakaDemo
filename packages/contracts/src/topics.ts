/** Kafka topic names used by the pipeline. */
export const Topics = {
  OrdersCreated: 'orders.created',
  PaymentsCompleted: 'payments.completed',
  PaymentsFailed: 'payments.failed',
  InventoryReserved: 'inventory.reserved',
  InventoryRejected: 'inventory.rejected',
} as const;

export type Topic = (typeof Topics)[keyof typeof Topics];

export const ALL_TOPICS: readonly Topic[] = Object.values(Topics);

export const DLQ_SUFFIX = '.dlq';

/** Dead-letter topic for a source topic: `<topic>.dlq`. */
export function dlqTopic(topic: string): string {
  return `${topic}${DLQ_SUFFIX}`;
}

export function isDlqTopic(topic: string): boolean {
  return topic.endsWith(DLQ_SUFFIX);
}

/** Source topic for a dead-letter topic (`orders.created.dlq` -> `orders.created`). */
export function sourceTopicOf(dlq: string): string {
  if (!isDlqTopic(dlq)) throw new Error(`"${dlq}" is not a DLQ topic`);
  return dlq.slice(0, -DLQ_SUFFIX.length);
}

export interface TopicSpec {
  topic: string;
  numPartitions: number;
}

/**
 * Every topic the pipeline needs, including DLQs. All business topics are keyed by
 * orderId and have 3 partitions, so every event for one order lands on the same
 * partition and is consumed in order.
 */
export const TOPIC_SPECS: readonly TopicSpec[] = ALL_TOPICS.flatMap((topic) => [
  { topic, numPartitions: 3 },
  { topic: dlqTopic(topic), numPartitions: 1 },
]);

/** Consumer group ids - one per service, so every service sees every event. */
export const ConsumerGroups = {
  PaymentService: 'payment-service',
  InventoryService: 'inventory-service',
  OrderService: 'order-service',
  NotificationService: 'notification-service',
} as const;

/** Kafka header names set on every event we produce. */
export const Headers = {
  EventId: 'event-id',
  EventType: 'event-type',
  EventVersion: 'event-version',
  CorrelationId: 'correlation-id',
  /** Set by tools/dlq-replay on replayed messages. */
  ReplayedFrom: 'replayed-from',
  ReplayCount: 'replay-count',
} as const;
