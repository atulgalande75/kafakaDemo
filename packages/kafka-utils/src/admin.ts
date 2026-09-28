import type { Kafka } from 'kafkajs';
import type { Logger } from 'pino';
import { TOPIC_SPECS, type TopicSpec } from '@orderflow/contracts';

/**
 * Creates any missing topics. Safe to call concurrently from several services:
 * a "topic already exists" race is fine as long as every topic exists afterwards.
 */
export async function ensureTopics(
  kafka: Kafka,
  logger: Logger,
  specs: readonly TopicSpec[] = TOPIC_SPECS,
): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = new Set(await admin.listTopics());
    const missing = specs.filter((s) => !existing.has(s.topic));
    if (missing.length === 0) return;

    try {
      await admin.createTopics({
        waitForLeaders: true,
        topics: missing.map(({ topic, numPartitions }) => ({ topic, numPartitions })),
      });
      logger.info({ topics: missing.map((s) => s.topic) }, 'created topics');
    } catch (err) {
      const now = new Set(await admin.listTopics());
      const stillMissing = missing.filter((s) => !now.has(s.topic));
      if (stillMissing.length > 0) throw err;
    }
  } finally {
    await admin.disconnect();
  }
}
