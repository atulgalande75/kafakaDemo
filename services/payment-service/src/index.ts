import { ConsumerGroups } from '@orderflow/contracts';
import {
  createFeatureFlags,
  eventContext,
  serviceContext,
  watchFlag,
} from '@orderflow/feature-flags';
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
import { createPaymentHandlers, type StaticPaymentSettings } from './handlers.js';

const SERVICE = 'payment-service';
const logger = createLogger(SERVICE);

async function main() {
  const settings: StaticPaymentSettings = {
    declineRate: envRate('PAYMENT_DECLINE_RATE', 0),
    cardLimit: envNumber('PAYMENT_CARD_LIMIT', 2000, { min: 0 }),
  };
  if (process.env.PAYMENT_FAILURE_RATE !== undefined) {
    logger.warn(
      'PAYMENT_FAILURE_RATE is no longer used - set the payment-failure-rate flag instead ' +
        '(feature-flags.json, FLAG_PAYMENT_FAILURE_RATE or LaunchDarkly)',
    );
  }
  const retry = retryPolicyFromEnv();
  logger.info({ ...settings, retry }, 'payment settings');

  const flags = await createFeatureFlags({ logger, service: SERVICE });
  flags.onChange((key) => {
    if (key !== 'payment-failure-rate') return;
    void flags.get(key, serviceContext(SERVICE)).then((rate) => {
      if (rate > 0) logger.warn(`CHAOS ON: ${rate * 100}% of gateway calls will fail`);
      else logger.info('chaos off');
    });
  });
  const initialRate = await flags.get('payment-failure-rate', serviceContext(SERVICE));
  if (initialRate > 0) logger.warn(`CHAOS ON: ${initialRate * 100}% of gateway calls will fail`);

  const kafka = createKafka({ clientId: SERVICE, logger });
  await ensureTopics(kafka, logger);

  const producer = createEventProducer(kafka, logger);
  await producer.connect();

  const consumer = createEventConsumer({
    kafka,
    groupId: ConsumerGroups.PaymentService,
    producer,
    logger,
    retry,
    maxRetries: (event) => flags.get('max-retry-attempts', eventContext(event)),
    handlers: createPaymentHandlers(producer, flags, settings),
  });

  // Kill switch: the payment-consumer-enabled flag pauses/resumes consumption live.
  await watchFlag(flags, 'payment-consumer-enabled', serviceContext(SERVICE), (enabled) =>
    enabled ? consumer.resume() : consumer.pause(),
  );

  onShutdown(
    logger,
    () => consumer.stop(),
    () => producer.disconnect(),
    () => flags.close(),
  );
  await consumer.start();
}

main().catch(exitOnError(logger));
