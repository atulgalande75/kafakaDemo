import { ConsumerGroups, Topics } from '@orderflow/contracts';
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

/** "Sends" a notification - for the demo, it is simply logged. */
const notify = (event: OutcomeEvent, { log }: HandlerContext) => {
  const notification = renderNotification(event);
  log.info(
    { orderId: notification.orderId, channel: notification.channel, body: notification.body },
    `notification: ${notification.subject}`,
  );
  return Promise.resolve();
};

async function main() {
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
  );
  await consumer.start();
}

main().catch(exitOnError(logger));
