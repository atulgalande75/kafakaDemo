import { randomUUID } from 'node:crypto';
import type { Message } from 'kafkajs';

/**
 * Messages that can never be processed. The consumer wrapper must route them to
 * orders.created.dlq instead of crashing or blocking the partition.
 */
export function poisonMessages(count: number): Message[] {
  const variants: Array<(n: number) => Buffer | string> = [
    // 1. Not JSON at all
    (n) => `this is not json #${n} {`,
    // 2. JSON, but not an event envelope
    (n) => JSON.stringify({ hello: 'world', n }),
    // 3. Valid envelope, invalid payload (no items, negative total)
    (n) =>
      JSON.stringify({
        eventId: randomUUID(),
        type: 'order.created',
        version: 1,
        occurredAt: new Date().toISOString(),
        correlationId: `poison-${n}`,
        data: {
          orderId: randomUUID(),
          customerId: 'x',
          items: [],
          totalAmount: -5,
          currency: 'USD',
        },
      }),
    // 4. Unsupported schema version
    (n) =>
      JSON.stringify({
        eventId: randomUUID(),
        type: 'order.created',
        version: 99,
        occurredAt: new Date().toISOString(),
        correlationId: `poison-${n}`,
        data: {},
      }),
    // 5. Binary garbage
    () => Buffer.from([0xde, 0xad, 0xbe, 0xef, 0x00, 0xff]),
  ];

  return Array.from({ length: count }, (_, n) => ({
    key: `poison-${n}`,
    value: variants[n % variants.length]!(n),
    headers: { 'correlation-id': `poison-${n}` },
  }));
}
