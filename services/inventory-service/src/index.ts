import { ConsumerGroups } from '@orderflow/contracts';
import {
  createEventConsumer,
  createEventProducer,
  createKafka,
  createLogger,
  ensureTopics,
  exitOnError,
  onShutdown,
  retryPolicyFromEnv,
} from '@orderflow/kafka-utils';
import { createInventoryHandlers } from './handlers.js';
import { Inventory } from './inventory.js';

const logger = createLogger('inventory-service');

async function main() {
  const inventory = new Inventory();
  logger.info({ stock: inventory.snapshot() }, 'initial stock');

  const kafka = createKafka({ clientId: 'inventory-service', logger });
  await ensureTopics(kafka, logger);

  const producer = createEventProducer(kafka, logger);
  await producer.connect();

  const consumer = createEventConsumer({
    kafka,
    groupId: ConsumerGroups.InventoryService,
    producer,
    logger,
    retry: retryPolicyFromEnv(),
    handlers: createInventoryHandlers(producer, inventory),
  });
  onShutdown(
    logger,
    () => consumer.stop(),
    () => producer.disconnect(),
  );
  await consumer.start();
}

main().catch(exitOnError(logger));
