import { init, type LDClient, type LDLogger, type LDOptions } from '@launchdarkly/node-server-sdk';
import type { Logger } from 'pino';
import { serviceContext, toLdContext, type FlagContext } from './context.js';
import {
  FLAGS,
  FLAG_KEYS,
  isFlagKey,
  sanitize,
  type FlagKey,
  type FlagValues,
} from './definitions.js';
import { ChangeEmitter, type FeatureFlags } from './feature-flags.js';

export interface LaunchDarklyFlagsOptions {
  sdkKey: string;
  logger: Logger;
  /** Name of the running service - the context used to log flag changes. */
  service: string;
  /** How long start-up waits for LaunchDarkly before carrying on with safe defaults. */
  initTimeoutSec?: number;
  /** Extra SDK options (tests use TestData via `updateProcessor`). */
  clientOptions?: LDOptions;
}

function ldLogger(logger: Logger): LDLogger {
  const log = logger.child({ component: 'launchdarkly' });
  const fmt = (args: unknown[]) => args.map(String).join(' ');
  return {
    error: (...args) => log.warn(fmt(args)), // SDK errors are recoverable; we fall back to defaults
    warn: (...args) => log.warn(fmt(args)),
    info: (...args) => log.debug(fmt(args)),
    debug: (...args) => log.debug(fmt(args)),
  };
}

/**
 * LaunchDarkly server-side SDK behind the {@link FeatureFlags} interface.
 * If LaunchDarkly is unreachable at start-up we log a warning and keep going: the
 * SDK serves our safe defaults until it connects, and keeps serving the last known
 * values if the connection drops later.
 */
export class LaunchDarklyFeatureFlags implements FeatureFlags {
  readonly provider = 'launchdarkly';
  private readonly emitter = new ChangeEmitter();
  private readonly client: LDClient;
  private readonly lastKnown = new Map<FlagKey, unknown>();

  constructor(private readonly options: LaunchDarklyFlagsOptions) {
    this.client = init(options.sdkKey, {
      logger: ldLogger(options.logger),
      ...options.clientOptions,
    });
  }

  async start(): Promise<this> {
    const { logger, initTimeoutSec = 5 } = this.options;
    try {
      await this.client.waitForInitialization({ timeout: initTimeoutSec });
      logger.info('connected to LaunchDarkly');
    } catch (err) {
      logger.warn(
        { err: (err as Error).message },
        'LaunchDarkly unavailable - serving safe defaults until it connects',
      );
    }

    const context = serviceContext(this.options.service);
    for (const key of FLAG_KEYS) this.lastKnown.set(key, await this.get(key, context));
    logger.info({ flags: Object.fromEntries(this.lastKnown) }, 'feature flags (service context)');

    this.client.on('update', ({ key }: { key: string }) => {
      if (isFlagKey(key)) void this.handleUpdate(key);
    });
    return this;
  }

  async get<K extends FlagKey>(key: K, context: FlagContext): Promise<FlagValues[K]> {
    const fallback = FLAGS[key].defaultValue;
    // Not connected (yet, or ever - e.g. invalid key): serve the safe default quietly
    // instead of letting the SDK warn on every single evaluation.
    if (!this.client.initialized()) return fallback;
    try {
      const raw: unknown = await this.client.variation(key, toLdContext(context), fallback);
      const { value, problem } = sanitize(key, raw);
      if (problem) {
        this.options.logger.warn(
          { flag: key, problem },
          'invalid flag value from LaunchDarkly, using safe default',
        );
      }
      return value;
    } catch (err) {
      this.options.logger.warn(
        { flag: key, err: (err as Error).message },
        'flag evaluation failed, using safe default',
      );
      return fallback;
    }
  }

  onChange(listener: (key: FlagKey) => void): () => void {
    return this.emitter.on(listener);
  }

  async close(): Promise<void> {
    await this.client.flush().catch(() => undefined);
    this.client.close();
  }

  private async handleUpdate(key: FlagKey): Promise<void> {
    const next = await this.get(key, serviceContext(this.options.service));
    const previous = this.lastKnown.get(key);
    this.lastKnown.set(key, next);
    this.options.logger.info(
      { flag: key, from: previous, to: next, provider: this.provider, context: 'service' },
      next === previous ? 'feature flag updated (targeting rules changed)' : 'feature flag changed',
    );
    // Always notify: targeting for order contexts may have changed even if the
    // service-context value did not.
    this.emitter.emit(key);
  }
}
