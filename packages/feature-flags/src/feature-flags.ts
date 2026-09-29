import type { FlagContext } from './context.js';
import type { FlagKey, FlagValues } from './definitions.js';

export interface FeatureFlags {
  readonly provider: 'launchdarkly' | 'local';
  /** Never throws: on any problem the flag's safe default is returned. */
  get<K extends FlagKey>(key: K, context: FlagContext): Promise<FlagValues[K]>;
  /** Called when a flag's configuration may have changed. Returns an unsubscribe function. */
  onChange(listener: (key: FlagKey) => void): () => void;
  close(): Promise<void>;
}

/** Small listener registry shared by the providers. */
export class ChangeEmitter {
  private readonly listeners = new Set<(key: FlagKey) => void>();

  on(listener: (key: FlagKey) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  emit(key: FlagKey): void {
    for (const listener of this.listeners) listener(key);
  }
}

/**
 * Evaluates `key` now and again whenever it changes, calling `apply` with the
 * current value on start and on every actual change (e.g. pause/resume a consumer).
 */
export async function watchFlag<K extends FlagKey>(
  flags: FeatureFlags,
  key: K,
  context: FlagContext,
  apply: (value: FlagValues[K], previous: FlagValues[K] | undefined) => void,
): Promise<() => void> {
  let current = await flags.get(key, context);
  apply(current, undefined);
  return flags.onChange((changed) => {
    if (changed !== key) return;
    void flags.get(key, context).then((next) => {
      if (next === current) return;
      const previous = current;
      current = next;
      apply(next, previous);
    });
  });
}
