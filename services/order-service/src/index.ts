import { authConfigFromEnv, createJwtVerifier, remoteJwks } from '@orderflow/auth';
import { ConsumerGroups } from '@orderflow/contracts';
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
import { createOutcomeHandlers } from './handlers.js';
import { InMemoryOrderRepository } from './repository.js';

const logger = createLogger('order-service');

async function main() {
  const port = envNumber('ORDER_SERVICE_PORT', 3000, { min: 1, max: 65535 });
  const kafka = createKafka({ clientId: 'order-service', logger });
  await ensureTopics(kafka, logger);

  const producer = createEventProducer(kafka, logger);
  await producer.connect();

  const repo = new InMemoryOrderRepository();
  const consumer = createEventConsumer({
    kafka,
    groupId: ConsumerGroups.OrderService,
    producer,
    logger,
    retry: retryPolicyFromEnv(),
    handlers: createOutcomeHandlers(repo),
  });

  const auth = authConfigFromEnv();
  logger.info({ issuer: auth.issuer, audience: auth.audience }, 'verifying JWTs via JWKS');
  const verifier = createJwtVerifier({
    issuer: auth.issuer,
    audience: auth.audience,
    jwks: remoteJwks(auth.jwksUri),
  });

  const app = buildApp({ repo, producer, logger, verifier });
  onShutdown(
    logger,
    () => app.close(),
    () => consumer.stop(),
    () => producer.disconnect(),
  );

  await consumer.start();
  await app.listen({ port, host: '0.0.0.0' });
}

main().catch(exitOnError(logger));
