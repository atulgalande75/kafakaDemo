import type { OrderCreatedEvent } from '@orderflow/contracts';
import type { Order } from './order.js';

export interface StoredOrder {
  order: Order;
  /** The exact event published for this order - kept so it can be re-published (demo). */
  createdEvent: OrderCreatedEvent;
}

export interface OrderRepository {
  get(id: string): Promise<StoredOrder | undefined>;
  save(stored: StoredOrder): Promise<void>;
  delete(id: string): Promise<void>;
  list(limit: number): Promise<Order[]>;
}

/**
 * In-memory repository: orders are lost when order-service restarts. Swap in a
 * database-backed implementation (and a transactional outbox) for real use.
 */
export class InMemoryOrderRepository implements OrderRepository {
  private readonly orders = new Map<string, StoredOrder>();

  get(id: string) {
    return Promise.resolve(this.orders.get(id));
  }

  save(stored: StoredOrder) {
    this.orders.set(stored.order.id, stored);
    return Promise.resolve();
  }

  delete(id: string) {
    this.orders.delete(id);
    return Promise.resolve();
  }

  list(limit: number) {
    return Promise.resolve(
      [...this.orders.values()]
        .map((s) => s.order)
        .reverse()
        .slice(0, limit),
    );
  }
}
