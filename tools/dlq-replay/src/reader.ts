import type { Kafka, KafkaMessage } from 'kafkajs';

export interface PendingMessage {
  partition: number;
  message: KafkaMessage;
}

/**
 * Reads every message currently in `topic` that `groupId` has not consumed yet,
 * then stops. Messages that arrive after we start are left for the next run.
 *
 * `onMessage` returns whether to continue. When `commit` is true each handled
 * message's offset is committed right away, so a replayed record is never
 * replayed twice by the same group; with `commit: false` it's a read-only peek.
 */
export async function readPending(
  kafka: Kafka,
  opts: {
    topic: string;
    groupId: string;
    commit: boolean;
    onMessage: (msg: PendingMessage) => Promise<boolean>;
  },
): Promise<{ pending: number }> {
  const { topic, groupId, commit } = opts;
  const admin = kafka.admin();
  await admin.connect();
  let end: Map<number, bigint>;
  let start: Map<number, bigint>;
  try {
    const watermarks = await admin.fetchTopicOffsets(topic);
    end = new Map(watermarks.map((w) => [w.partition, BigInt(w.high)]));
    const low = new Map(watermarks.map((w) => [w.partition, BigInt(w.low)]));
    const [committed] = await admin.fetchOffsets({ groupId, topics: [topic] });
    start = new Map(
      watermarks.map((w) => {
        const c = committed?.partitions.find((p) => p.partition === w.partition);
        const offset = c && Number(c.offset) >= 0 ? BigInt(c.offset) : low.get(w.partition)!;
        return [w.partition, offset > low.get(w.partition)! ? offset : low.get(w.partition)!];
      }),
    );
  } finally {
    await admin.disconnect();
  }

  const remaining = new Set([...end].filter(([p, high]) => start.get(p)! < high).map(([p]) => p));
  const pending = [...remaining].reduce((sum, p) => sum + Number(end.get(p)! - start.get(p)!), 0);
  if (remaining.size === 0) return { pending: 0 };

  const consumer = kafka.consumer({ groupId });
  let finish!: () => void;
  let fail!: (err: unknown) => void;
  const done = new Promise<void>((resolve, reject) => {
    finish = resolve;
    fail = reject;
  });
  let stopped = false;

  await consumer.connect();
  try {
    await consumer.subscribe({ topic, fromBeginning: true });
    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ partition, message }) => {
        if (stopped || !remaining.has(partition)) return;
        const offset = BigInt(message.offset);
        if (offset >= end.get(partition)!) {
          remaining.delete(partition);
        } else {
          try {
            const keepGoing = await opts.onMessage({ partition, message });
            if (commit) {
              await consumer.commitOffsets([
                { topic, partition, offset: (offset + 1n).toString() },
              ]);
            }
            if (offset + 1n >= end.get(partition)!) remaining.delete(partition);
            if (!keepGoing) stopped = true;
          } catch (err) {
            stopped = true;
            fail(err);
            return;
          }
        }
        if (stopped || remaining.size === 0) finish();
      },
    });
    await done;
  } finally {
    await consumer.disconnect();
  }
  return { pending };
}
