import { ConsumerGroups, Topics } from '@orderflow/contracts';
import {
  createFeatureFlags,
  eventContext,
  orderContext,
  type FeatureFlags,
} from '@orderflow/feature-flags';
import {
  createEventConsumer,
  createEventProducer,
  createKafka,
  createLogger,
  ensureTopics,
  exitOnError,
  onShutdown,
  retryPolicyFromEnv,
  type HandlerContext,
} from '@orderflow/kafka-utils';
import { renderNotification, type OutcomeEvent } from './notifications.js';

const logger = createLogger('notification-service');

/** "Sends" a notification on the channel chosen by the notification-channel flag - logged for the demo. */
const createNotifier =
  (flags: FeatureFlags) =>
  async (event: OutcomeEvent, { log }: HandlerContext) => {
    const channel = await flags.get('notification-channel', orderContext(event.data));
    const notification = renderNotification(event, channel);
    log.info(
      { orderId: notification.orderId, channel, body: notification.body },
      `[${channel}] ${notification.subject}`,
    );
  };

async function main() {
  const flags = await createFeatureFlags({ logger, service: 'notification-service' });
  const notify = createNotifier(flags);

  const kafka = createKafka({ clientId: 'notification-service', logger });
  await ensureTopics(kafka, logger);

  // The producer is only used to publish to DLQ topics.
  const producer = createEventProducer(kafka, logger);
  await producer.connect();

  const consumer = createEventConsumer({
    kafka,
    groupId: ConsumerGroups.NotificationService,
    producer,
    logger,
    retry: retryPolicyFromEnv(),
    maxRetries: (event) => flags.get('max-retry-attempts', eventContext(event)),
    handlers: {
      [Topics.PaymentsCompleted]: notify,
      [Topics.PaymentsFailed]: notify,
      [Topics.InventoryReserved]: notify,
      [Topics.InventoryRejected]: notify,
    },
  });
  onShutdown(
    logger,
    () => consumer.stop(),
    () => producer.disconnect(),
    () => flags.close(),
  );
  await consumer.start();
}

main().catch(exitOnError(logger));
