import { ConsumerGroups } from '@orderflow/contracts';
import {
  createEventConsumer,
  createEventProducer,
  createKafka,
  createLogger,
  ensureTopics,
  envNumber,
  envRate,
  exitOnError,
  onShutdown,
  retryPolicyFromEnv,
} from '@orderflow/kafka-utils';
import { createPaymentHandlers } from './handlers.js';
import type { PaymentSettings } from './payment.js';

const logger = createLogger('payment-service');

async function main() {
  const settings: PaymentSettings = {
    failureRate: envRate('PAYMENT_FAILURE_RATE', 0),
    declineRate: envRate('PAYMENT_DECLINE_RATE', 0.1),
    cardLimit: envNumber('PAYMENT_CARD_LIMIT', 2000, { min: 0 }),
  };
  const retry = retryPolicyFromEnv();
  logger.info({ ...settings, retry }, 'payment settings');
  if (settings.failureRate > 0) {
    logger.warn(`CHAOS ON: ${settings.failureRate * 100}% of gateway calls will fail`);
  }

  const kafka = createKafka({ clientId: 'payment-service', logger });
  await ensureTopics(kafka, logger);

  const producer = createEventProducer(kafka, logger);
  await producer.connect();

  const consumer = createEventConsumer({
    kafka,
    groupId: ConsumerGroups.PaymentService,
    producer,
    logger,
    retry,
    handlers: createPaymentHandlers(producer, settings),
  });
  onShutdown(
    logger,
    () => consumer.stop(),
    () => producer.disconnect(),
  );
  await consumer.start();
}

main().catch(exitOnError(logger));
