import { fileURLToPath } from 'node:url';
import type { Logger } from 'pino';
import { serviceContext } from './context.js';
import { FLAG_KEYS } from './definitions.js';
import type { FeatureFlags } from './feature-flags.js';
import { LaunchDarklyFeatureFlags } from './launchdarkly.js';
import { LocalFeatureFlags } from './local.js';

/** `feature-flags.json` at the repository root (works from src/ and dist/). */
export const DEFAULT_FLAGS_FILE = fileURLToPath(
  new URL('../../../feature-flags.json', import.meta.url),
);

/**
 * LaunchDarkly when LD_SDK_KEY is set, otherwise local flags from FEATURE_FLAGS_FILE
 * (default: ./feature-flags.json) and FLAG_* environment variables.
 */
export async function createFeatureFlags(options: {
  logger: Logger;
  service: string;
}): Promise<FeatureFlags> {
  const logger = options.logger.child({ component: 'feature-flags' });
  const sdkKey = process.env.LD_SDK_KEY?.trim();
  if (sdkKey) {
    logger.info('using LaunchDarkly (LD_SDK_KEY is set)');
    return new LaunchDarklyFeatureFlags({ sdkKey, logger, service: options.service }).start();
  }

  const file = process.env.FEATURE_FLAGS_FILE?.trim() || DEFAULT_FLAGS_FILE;
  const flags = new LocalFeatureFlags({ logger, file });
  const context = serviceContext(options.service);
  const values: Record<string, unknown> = {};
  for (const key of FLAG_KEYS) values[key] = await flags.get(key, context);
  logger.info(
    { file, flags: values },
    'LD_SDK_KEY not set - using local feature flags (edit the file to change them live)',
  );
  return flags;
}
