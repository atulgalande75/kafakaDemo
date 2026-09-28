import { Kafka, logLevel, type LogEntry } from 'kafkajs';
import type { Logger } from 'pino';
import { brokersFromEnv } from './config.js';

export interface CreateKafkaOptions {
  clientId: string;
  logger: Logger;
  brokers?: string[];
}

const PINO_LEVEL: Record<logLevel, 'error' | 'warn' | 'info' | 'debug' | undefined> = {
  [logLevel.NOTHING]: undefined,
  [logLevel.ERROR]: 'error',
  [logLevel.WARN]: 'warn',
  [logLevel.INFO]: 'info',
  [logLevel.DEBUG]: 'debug',
};

/** Creates a kafkajs client whose internal logs go through pino. */
export function createKafka({ clientId, logger, brokers = brokersFromEnv() }: CreateKafkaOptions) {
  const kafkaLogger = logger.child({ component: 'kafkajs' });
  return new Kafka({
    clientId,
    brokers,
    // kafkajs is chatty at INFO; the wrappers log the interesting lifecycle events themselves.
    logLevel: process.env.KAFKAJS_DEBUG ? logLevel.DEBUG : logLevel.WARN,
    logCreator:
      () =>
      ({ namespace, level, log }: LogEntry) => {
        const pinoLevel = PINO_LEVEL[level];
        if (!pinoLevel) return;
        const { message, ...extra } = log;
        kafkaLogger[pinoLevel]({ namespace, ...extra }, message);
      },
    // Keep retrying the initial connection so services can start before Kafka is ready.
    retry: { initialRetryTime: 300, maxRetryTime: 10_000, retries: 15 },
  });
}
