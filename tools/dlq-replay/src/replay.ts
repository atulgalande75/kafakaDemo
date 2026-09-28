import type { Message } from 'kafkajs';
import { Headers, dlqRecordSchema, type DlqRecord } from '@orderflow/contracts';

export interface ReplayFilter {
  /** Only replay records dead-lettered by this consumer group. */
  consumerGroup?: string;
  /** 'all' or a specific DLQ reason. */
  reason: DlqRecord['reason'] | 'all';
}

export function parseDlqRecord(value: Buffer | null): DlqRecord {
  if (!value) throw new Error('empty DLQ record');
  return dlqRecordSchema.parse(JSON.parse(value.toString()));
}

export function matchesFilter(record: DlqRecord, filter: ReplayFilter): boolean {
  if (filter.reason !== 'all' && record.reason !== filter.reason) return false;
  if (filter.consumerGroup && record.consumerGroup !== filter.consumerGroup) return false;
  return true;
}

/**
 * Rebuilds the original message byte-for-byte (same key, value and headers - so
 * the same eventId) and adds headers recording where it was replayed from.
 */
export function toReplayMessage(
  record: DlqRecord,
  source: { topic: string; partition: number; offset: string },
): Message {
  const { key, headers, value, valueEncoding } = record.original;
  const previousReplays = Number(headers[Headers.ReplayCount] ?? 0);
  return {
    key,
    value: value === null ? null : Buffer.from(value, valueEncoding),
    headers: {
      ...headers,
      [Headers.ReplayedFrom]: `${source.topic}/${source.partition}@${source.offset}`,
      [Headers.ReplayCount]: String(previousReplays + 1),
    },
  };
}

export function describeRecord(record: DlqRecord, offset: string): string {
  const { original, error } = record;
  return (
    `#${offset} ${record.reason} by ${record.consumerGroup} after ${record.attempts} attempt(s)` +
    ` | ${original.topic}[${original.partition}]@${original.offset} key=${original.key ?? '∅'}` +
    ` | ${error.name}: ${error.message.split('\n')[0]}`
  );
}
