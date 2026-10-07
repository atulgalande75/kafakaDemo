import { authConfigFromEnv, createJwtVerifier, remoteJwks } from '@orderflow/auth';
import { ConsumerGroups } from '@orderflow/contracts';
import { createFeatureFlags, eventContext } from '@orderflow/feature-flags';
import {
  createEventConsumer,
  createEventProducer,
  createKafka,
  createLogger,
  ensureTopics,
  envNumber,
  exitOnError,
  onShutdown,
  retryPolicyFromEnv,
} from '@orderflow/kafka-utils';
import { buildApp } from './app.js';
import { createPgDb, describeDatabase, DEFAULT_DATABASE_URL } from './db.js';
import { createInventoryHandlers } from './handlers.js';
import { InventoryStore } from './inventory.js';
import { migrate } from './migrations.js';
import { OutboxRelay } from './outbox.js';

const logger = createLogger('inventory-service');

async function main() {
  const port = envNumber('INVENTORY_SERVICE_PORT', 3001, { min: 1, max: 65535 });

  const databaseUrl = process.env.DATABASE_URL?.trim() || DEFAULT_DATABASE_URL;
  const db = createPgDb(databaseUrl);
  await migrate(db, logger);
  const inventory = new InventoryStore(db);
  const seeded = await inventory.seed();
  logger.info(
    { database: describeDatabase(databaseUrl), seeded, stock: await inventory.list() },
    'inventory database ready',
  );

  const flags = await createFeatureFlags({ logger, service: 'inventory-service' });
  const kafka = createKafka({ clientId: 'inventory-service', logger });
  await ensureTopics(kafka, logger);

  const producer = createEventProducer(kafka, logger);
  await producer.connect();

  // Events reach Kafka only through the outbox relay, never directly from a handler.
  const relay = new OutboxRelay(db, producer, logger);
  relay.start();
  const snapshotted = await inventory.publishSnapshot();
  relay.poke();
  logger.info({ skus: snapshotted }, 'published stock snapshot to inventory.stock-levels');

  const consumer = createEventConsumer({
    kafka,
    groupId: ConsumerGroups.InventoryService,
    producer,
    logger,
    retry: retryPolicyFromEnv(),
    maxRetries: (event) => flags.get('max-retry-attempts', eventContext(event)),
    handlers: createInventoryHandlers(inventory, relay),
  });

  const auth = authConfigFromEnv({
    audienceEnv: 'INVENTORY_AUTH_AUDIENCE',
    defaultAudience: 'inventory-service',
  });
  logger.info({ issuer: auth.issuer, audience: auth.audience }, 'verifying JWTs via JWKS');
  const app = buildApp({
    inventory,
    relay,
    logger,
    verifier: createJwtVerifier({
      issuer: auth.issuer,
      audience: auth.audience,
      jwks: remoteJwks(auth.jwksUri),
    }),
  });

  onShutdown(
    logger,
    () => app.close(),
    () => consumer.stop(),
    () => relay.stop(),
    () => producer.disconnect(),
    () => flags.close(),
    () => db.close(),
  );

  await consumer.start();
  await app.listen({ port, host: '0.0.0.0' });
}

main().catch(exitOnError(logger));
