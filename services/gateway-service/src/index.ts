import { randomUUID } from 'node:crypto';
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
  envString,
  exitOnError,
  onShutdown,
  retryPolicyFromEnv,
} from '@orderflow/kafka-utils';
import { buildApp } from './app.js';
import { createFeedHandlers, createStateHandlers } from './handlers.js';
import { Hub } from './hub.js';

const logger = createLogger('gateway-service');

async function main() {
  const port = envNumber('GATEWAY_SERVICE_PORT', 3002, { min: 1, max: 65535 });
  const allowedOrigins = envString('WEB_ORIGIN', 'http://localhost:5173')
    .split(',')
    .map((o) => o.trim())
    .filter(Boolean);
  // Every gateway instance has to see every event to serve its own connected clients, so each
  // one uses its own consumer groups (a shared group would split the partitions between them).
  const instanceId = envString('GATEWAY_INSTANCE_ID', randomUUID().slice(0, 8));

  const flags = await createFeatureFlags({ logger, service: 'gateway-service' });
  const kafka = createKafka({ clientId: `gateway-service-${instanceId}`, logger });
  await ensureTopics(kafka, logger);

  // Only used to dead-letter messages the gateway can't decode.
  const producer = createEventProducer(kafka, logger);
  await producer.connect();

  const hub = new Hub();
  const common = {
    kafka,
    producer,
    logger,
    retry: retryPolicyFromEnv(),
    maxRetries: (event: Parameters<typeof eventContext>[0]) =>
      flags.get('max-retry-attempts', eventContext(event)),
  };
  const groupBase = `${ConsumerGroups.GatewayServicePrefix}-${instanceId}`;

  // Stock: replay the compacted changelog from the start to rebuild the table.
  const stateConsumer = createEventConsumer({
    ...common,
    groupId: `${groupBase}-state`,
    fromBeginning: true,
    handlers: createStateHandlers(hub),
  });
  // Feed: only what happens from now on.
  const feedConsumer = createEventConsumer({
    ...common,
    groupId: `${groupBase}-feed`,
    fromBeginning: false,
    handlers: createFeedHandlers(hub),
  });

  const auth = authConfigFromEnv({
    audienceEnv: 'GATEWAY_AUTH_AUDIENCE',
    defaultAudience: 'gateway-service',
  });
  logger.info({ issuer: auth.issuer, audience: auth.audience }, 'verifying JWTs via JWKS');
  const app = buildApp({
    hub,
    flags,
    logger,
    allowedOrigins,
    verifier: createJwtVerifier({
      issuer: auth.issuer,
      audience: auth.audience,
      jwks: remoteJwks(auth.jwksUri),
    }),
  });

  onShutdown(
    logger,
    () => app.close(),
    () => stateConsumer.stop(),
    () => feedConsumer.stop(),
    () => producer.disconnect(),
    () => flags.close(),
  );

  await stateConsumer.start();
  await feedConsumer.start();
  await app.listen({ port, host: '0.0.0.0' });
  logger.info({ port, instanceId, allowedOrigins }, 'gateway listening');
}

main().catch(exitOnError(logger));
