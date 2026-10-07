/** Kafka topic names used by the pipeline. */
export const Topics = {
  OrdersCreated: 'orders.created',
  PaymentsCompleted: 'payments.completed',
  PaymentsFailed: 'payments.failed',
  InventoryReserved: 'inventory.reserved',
  InventoryRejected: 'inventory.rejected',
  InventoryReleased: 'inventory.released',
  /** Changelog of every SKU's stock level, keyed by SKU and log-compacted. */
  StockLevels: 'inventory.stock-levels',
  StockLow: 'inventory.stock-low',
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
  /** Broker-side topic configs, e.g. `{ 'cleanup.policy': 'compact' }`. */
  config?: Readonly<Record<string, string>>;
}

/** Topics that are not keyed by orderId: their key is the SKU. */
const STOCK_TOPICS: ReadonlySet<Topic> = new Set([Topics.StockLevels, Topics.StockLow]);

/** The partition key kind of a topic: events about stock are keyed by SKU, the rest by orderId. */
export function keyKindOf(topic: Topic): 'orderId' | 'sku' {
  return STOCK_TOPICS.has(topic) ? 'sku' : 'orderId';
}

/** Per-topic broker configs. `inventory.stock-levels` keeps only the latest record per SKU. */
const TOPIC_CONFIG: Partial<Record<Topic, Readonly<Record<string, string>>>> = {
  [Topics.StockLevels]: {
    'cleanup.policy': 'compact',
    // Demo-friendly values so compaction actually runs within minutes, not days.
    'min.cleanable.dirty.ratio': '0.1',
    'segment.ms': '60000',
    'delete.retention.ms': '60000',
  },
};

/**
 * Every topic the pipeline needs, including DLQs. Business topics have 3 partitions and
 * are keyed by orderId (order topics) or SKU (stock topics), so every event for one
 * order / one SKU lands on the same partition and is consumed in order.
 */
export const TOPIC_SPECS: readonly TopicSpec[] = ALL_TOPICS.flatMap((topic) => [
  { topic, numPartitions: 3, ...(TOPIC_CONFIG[topic] && { config: TOPIC_CONFIG[topic] }) },
  { topic: dlqTopic(topic), numPartitions: 1 },
]);

/** Consumer group ids - one per service, so every service sees every event. */
export const ConsumerGroups = {
  PaymentService: 'payment-service',
  InventoryService: 'inventory-service',
  OrderService: 'order-service',
  NotificationService: 'notification-service',
  /** gateway-service appends its instance id: every instance must see every event. */
  GatewayServicePrefix: 'gateway-service',
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
  /** OAuth client that replayed the message (identity only, never a token). */
  ReplayedBy: 'replayed-by',
} as const;
