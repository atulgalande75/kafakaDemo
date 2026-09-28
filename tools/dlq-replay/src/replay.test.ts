import { describe, expect, it } from 'vitest';
import type { DlqRecord } from '@orderflow/contracts';
import { describeRecord, matchesFilter, parseDlqRecord, toReplayMessage } from './replay.js';

const record = (overrides: Partial<DlqRecord> = {}): DlqRecord => ({
  reason: 'processing-failed',
  error: { name: 'PaymentGatewayError', message: 'Payment gateway timeout' },
  attempts: 4,
  failedAt: '2024-01-01T00:00:00.000Z',
  consumerGroup: 'payment-service',
  original: {
    topic: 'orders.created',
    partition: 2,
    offset: '17',
    timestamp: '1700000000000',
    key: 'order-1',
    headers: { 'event-id': 'e-1', 'correlation-id': 'c-1' },
    value: '{"eventId":"e-1"}',
    valueEncoding: 'utf8',
  },
  ...overrides,
});

describe('dlq-replay', () => {
  it('filters by reason and consumer group', () => {
    expect(matchesFilter(record(), { reason: 'processing-failed' })).toBe(true);
    expect(matchesFilter(record(), { reason: 'invalid-message' })).toBe(false);
    expect(matchesFilter(record(), { reason: 'all', consumerGroup: 'inventory-service' })).toBe(
      false,
    );
    expect(matchesFilter(record(), { reason: 'all', consumerGroup: 'payment-service' })).toBe(true);
  });

  it('rebuilds the original message and tags it as replayed', () => {
    const message = toReplayMessage(record(), {
      topic: 'orders.created.dlq',
      partition: 0,
      offset: '3',
    });
    expect(message.key).toBe('order-1');
    expect(message.value?.toString()).toBe('{"eventId":"e-1"}');
    expect(message.headers).toEqual({
      'event-id': 'e-1',
      'correlation-id': 'c-1',
      'replayed-from': 'orders.created.dlq/0@3',
      'replay-count': '1',
    });
  });

  it('increments replay-count and restores base64 values', () => {
    const base = record();
    const binary = Buffer.from([0xde, 0xad, 0xbe, 0xef]);
    const message = toReplayMessage(
      record({
        original: {
          ...base.original,
          headers: { 'replay-count': '2' },
          value: binary.toString('base64'),
          valueEncoding: 'base64',
        },
      }),
      { topic: 'orders.created.dlq', partition: 0, offset: '9' },
    );
    expect(message.headers?.['replay-count']).toBe('3');
    expect(Buffer.compare(message.value as Buffer, binary)).toBe(0);
  });

  it('parses and describes DLQ records', () => {
    const parsed = parseDlqRecord(Buffer.from(JSON.stringify(record())));
    expect(describeRecord(parsed, '5')).toBe(
      '#5 processing-failed by payment-service after 4 attempt(s) | orders.created[2]@17 key=order-1 | PaymentGatewayError: Payment gateway timeout',
    );
    expect(() => parseDlqRecord(Buffer.from('{}'))).toThrow();
  });
});
