#!/usr/bin/env node
/**
 * Inspect and replay dead-lettered messages.
 *
 *   npm run dlq:replay                                        # DLQ depth per topic
 *   npm run dlq:replay -- -t orders.created.dlq --dry-run     # list records, change nothing
 *   npm run dlq:replay -- -t orders.created.dlq --group payment-service
 *
 * Replayed messages are published to their original topic with their original
 * key, value and headers (so the same eventId) plus replayed-from/replay-count.
 */
import { randomUUID } from 'node:crypto';
import { parseArgs } from 'node:util';
import { TOPIC_SPECS, isDlqTopic, sourceTopicOf, type DlqRecord } from '@orderflow/contracts';
import { createEventProducer, createKafka, createLogger } from '@orderflow/kafka-utils';
import { readPending } from './reader.js';
import {
  describeRecord,
  matchesFilter,
  parseDlqRecord,
  toReplayMessage,
  type ReplayFilter,
} from './replay.js';

const USAGE = `Usage: npm run dlq:replay -- [options]

  (no options)             show how many messages each DLQ holds
  -t, --topic <dlq>        DLQ topic to replay, e.g. orders.created.dlq
  -g, --group <group>      only replay records dead-lettered by this consumer group
  -r, --reason <reason>    processing-failed (default) | invalid-message | all
  -l, --limit <n>          replay at most <n> records
      --to <topic>         publish to this topic instead of the original one
      --dry-run            list matching records without replaying or committing
      --from-beginning     re-read the whole DLQ, including records replayed before
  -h, --help

Progress is tracked by the consumer group "dlq-replay.<dlq topic>", so each record
is handled once; records that don't match the filters are skipped *and* marked
as handled (use --from-beginning to revisit them).`;

const REASONS = ['processing-failed', 'invalid-message', 'all'] as const;

const { values } = parseArgs({
  options: {
    topic: { type: 'string', short: 't' },
    group: { type: 'string', short: 'g' },
    reason: { type: 'string', short: 'r', default: 'processing-failed' },
    limit: { type: 'string', short: 'l' },
    to: { type: 'string' },
    'dry-run': { type: 'boolean', default: false },
    'from-beginning': { type: 'boolean', default: false },
    help: { type: 'boolean', short: 'h', default: false },
  },
});

// Keep CLI output focused on results; set LOG_LEVEL=info to see Kafka client logs.
process.env.LOG_LEVEL ??= 'warn';

const logger = createLogger('dlq-replay');
const kafka = createKafka({ clientId: 'dlq-replay', logger });

async function showDepths() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const rows: Record<string, { messages: number }> = {};
    for (const { topic } of TOPIC_SPECS.filter((s) => isDlqTopic(s.topic))) {
      if (!existing.has(topic)) continue;
      const offsets = await admin.fetchTopicOffsets(topic);
      rows[topic] = {
        messages: offsets.reduce((sum, o) => sum + Number(o.high) - Number(o.low), 0),
      };
    }
    console.table(rows);
    console.log('\nReplay with: npm run dlq:replay -- --topic <dlq> [--dry-run]  (see --help)');
  } finally {
    await admin.disconnect();
  }
}

async function replay(dlq: string) {
  if (!isDlqTopic(dlq))
    throw new Error(`--topic must be a DLQ topic (ending in .dlq), got "${dlq}"`);
  const reason = values.reason as ReplayFilter['reason'];
  if (!REASONS.includes(reason)) throw new Error(`--reason must be one of ${REASONS.join(', ')}`);
  const limit = values.limit === undefined ? Infinity : Number(values.limit);
  if (!(limit > 0)) throw new Error('--limit must be a positive number');

  const filter: ReplayFilter = { reason, consumerGroup: values.group };
  const dryRun = values['dry-run'];
  const groupId = dryRun ? `dlq-peek-${randomUUID()}` : `dlq-replay.${dlq}`;

  if (values['from-beginning'] && !dryRun) {
    const admin = kafka.admin();
    await admin.connect();
    try {
      await admin.resetOffsets({ groupId, topic: dlq, earliest: true });
    } catch {
      // The group doesn't exist yet - it will start from the beginning anyway.
    } finally {
      await admin.disconnect();
    }
  }

  const producer = createEventProducer(kafka, logger);
  if (!dryRun) await producer.connect();

  const counts = { replayed: 0, skipped: 0, unreadable: 0 };
  const byTarget: Record<string, number> = {};
  try {
    const { pending } = await readPending(kafka, {
      topic: dlq,
      groupId,
      commit: !dryRun,
      onMessage: async ({ partition, message }) => {
        let record: DlqRecord;
        try {
          record = parseDlqRecord(message.value);
        } catch (err) {
          counts.unreadable++;
          console.log(`  ? #${message.offset} unreadable DLQ record: ${(err as Error).message}`);
          return true;
        }
        if (!matchesFilter(record, filter)) {
          counts.skipped++;
          return true;
        }

        const target = values.to ?? record.original.topic ?? sourceTopicOf(dlq);
        if (dryRun) {
          console.log(`  • ${describeRecord(record, message.offset)}`);
        } else {
          await producer.sendRaw(target, [
            toReplayMessage(record, { topic: dlq, partition, offset: message.offset }),
          ]);
          console.log(`  ↻ ${describeRecord(record, message.offset)} -> ${target}`);
        }
        counts.replayed++;
        byTarget[target] = (byTarget[target] ?? 0) + 1;
        return counts.replayed < limit;
      },
    });

    if (pending === 0) {
      console.log(`Nothing to replay: ${dlq} has no records left for group "${groupId}".`);
      if (!dryRun) console.log('Use --from-beginning to re-read records handled earlier.');
      return;
    }
    const verb = dryRun ? 'would replay' : 'replayed';
    console.log(
      `\n${dryRun ? '🔍 DRY RUN - ' : '✅ '}${verb} ${counts.replayed}, ` +
        `skipped ${counts.skipped} (filter: reason=${reason}${filter.consumerGroup ? `, group=${filter.consumerGroup}` : ''})` +
        (counts.unreadable ? `, ${counts.unreadable} unreadable` : ''),
    );
    for (const [topic, n] of Object.entries(byTarget)) console.log(`   ${n} -> ${topic}`);
  } finally {
    if (!dryRun) await producer.disconnect();
  }
}

async function main() {
  if (values.help) {
    console.log(USAGE);
    return;
  }
  if (!values.topic) {
    await showDepths();
    return;
  }
  await replay(values.topic);
}

main().catch((err: unknown) => {
  console.error((err as Error).message ?? err);
  process.exit(1);
});
