/**
 * Remembers which eventIds a consumer group has already processed so that
 * redelivered or duplicated events are skipped.
 *
 * Kafka guarantees at-least-once delivery for consumers: after a crash or
 * rebalance, messages processed since the last committed offset are delivered
 * again. Producers retrying a send can also write the same event twice.
 */
export interface IdempotencyStore {
  has(eventId: string): Promise<boolean>;
  add(eventId: string): Promise<void>;
}

/**
 * Bounded in-memory store (oldest entries evicted first). Good enough for a demo;
 * state is lost on restart. In production use a durable store shared by all
 * instances of the service - e.g. a `processed_events` table written in the same
 * DB transaction as the side effect, or Redis `SET NX` with a TTL.
 */
export class InMemoryIdempotencyStore implements IdempotencyStore {
  private readonly seen = new Set<string>();

  constructor(private readonly maxEntries = 100_000) {}

  has(eventId: string): Promise<boolean> {
    return Promise.resolve(this.seen.has(eventId));
  }

  add(eventId: string): Promise<void> {
    this.seen.add(eventId);
    if (this.seen.size > this.maxEntries) {
      // Sets iterate in insertion order, so the first value is the oldest.
      const oldest = this.seen.values().next().value;
      if (oldest !== undefined) this.seen.delete(oldest);
    }
    return Promise.resolve();
  }

  get size(): number {
    return this.seen.size;
  }
}
