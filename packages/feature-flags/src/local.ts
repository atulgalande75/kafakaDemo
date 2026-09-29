import { existsSync, readFileSync, unwatchFile, watchFile } from 'node:fs';
import type { Logger } from 'pino';
import type { FlagContext } from './context.js';
import {
  FLAG_KEYS,
  defaultValues,
  isFlagKey,
  sanitize,
  type FlagKey,
  type FlagValues,
} from './definitions.js';
import { ChangeEmitter, type FeatureFlags } from './feature-flags.js';

export interface LocalFlagsOptions {
  logger: Logger;
  /** JSON file of `{ "<flag-key>": value }`, re-read when it changes. */
  file?: string;
  /** Poll interval for file changes (ms); 0 disables watching. */
  watchIntervalMs?: number;
  /** Environment for `FLAG_<KEY>` overrides, e.g. FLAG_PAYMENT_FAILURE_RATE=0.5. */
  env?: NodeJS.ProcessEnv;
  /** Fixed values (tests). */
  values?: Partial<Record<FlagKey, unknown>>;
}

export const envVarFor = (key: FlagKey) => `FLAG_${key.toUpperCase().replace(/-/g, '_')}`;

/**
 * Feature flags without LaunchDarkly: used in tests, CI and whenever LD_SDK_KEY is
 * unset. Precedence: FLAG_* env vars > constructor values > JSON file > safe defaults.
 * Values are the same for every context (no targeting). Editing the JSON file while
 * services run changes flags live, just like toggling them in LaunchDarkly.
 */
export class LocalFeatureFlags implements FeatureFlags {
  readonly provider = 'local';
  private readonly emitter = new ChangeEmitter();
  private readonly overrides: Partial<Record<FlagKey, unknown>>;
  private current: FlagValues;

  constructor(private readonly options: LocalFlagsOptions) {
    this.overrides = { ...options.values };
    this.current = this.compute();
    const { file, watchIntervalMs = 1000 } = options;
    if (file && watchIntervalMs > 0) {
      watchFile(file, { interval: watchIntervalMs, persistent: false }, () => this.reload());
    }
  }

  get<K extends FlagKey>(key: K, _context?: FlagContext): Promise<FlagValues[K]> {
    return Promise.resolve(this.current[key]);
  }

  onChange(listener: (key: FlagKey) => void): () => void {
    return this.emitter.on(listener);
  }

  /** Changes a flag at runtime (tests, demos). */
  set<K extends FlagKey>(key: K, value: FlagValues[K]): void {
    this.overrides[key] = value;
    this.reload();
  }

  /** Re-reads the file and env, logs and announces every flag whose value changed. */
  reload(): void {
    const previous = this.current;
    this.current = this.compute();
    for (const key of FLAG_KEYS) {
      if (previous[key] !== this.current[key]) {
        this.options.logger.info(
          { flag: key, from: previous[key], to: this.current[key], provider: this.provider },
          'feature flag changed',
        );
        this.emitter.emit(key);
      }
    }
  }

  close(): Promise<void> {
    if (this.options.file) unwatchFile(this.options.file);
    return Promise.resolve();
  }

  private compute(): FlagValues {
    const values = defaultValues() as unknown as Record<FlagKey, unknown>;
    const env = this.options.env ?? process.env;
    const layers: Array<[string, Partial<Record<string, unknown>>]> = [
      ['file', this.readFile()],
      ['values', this.overrides],
      ['env', Object.fromEntries(FLAG_KEYS.map((k) => [k, env[envVarFor(k)]]))],
    ];
    for (const [source, layer] of layers) {
      for (const [key, raw] of Object.entries(layer)) {
        if (raw === undefined) continue;
        if (!isFlagKey(key)) {
          this.options.logger.warn({ flag: key, source }, 'unknown feature flag ignored');
          continue;
        }
        const { value, problem } = sanitize(key, raw);
        if (problem) {
          this.options.logger.warn(
            { flag: key, source, problem },
            'invalid flag value, using safe default',
          );
        }
        values[key] = value;
      }
    }
    return values as unknown as FlagValues;
  }

  private readFile(): Record<string, unknown> {
    const { file, logger } = this.options;
    if (!file || !existsSync(file)) return {};
    try {
      const json: unknown = JSON.parse(readFileSync(file, 'utf8'));
      if (json === null || typeof json !== 'object' || Array.isArray(json)) {
        throw new Error('expected a JSON object');
      }
      return json as Record<string, unknown>;
    } catch (err) {
      logger.warn(
        { file, err: (err as Error).message },
        'cannot read feature flag file, ignoring it',
      );
      return {};
    }
  }
}
