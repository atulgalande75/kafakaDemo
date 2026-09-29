export const NOTIFICATION_CHANNELS = ['email', 'sms', 'push', 'slack'] as const;
export type NotificationChannel = (typeof NOTIFICATION_CHANNELS)[number];

/** The value type of every flag. */
export interface FlagValues {
  'payment-failure-rate': number;
  'payment-consumer-enabled': boolean;
  'fraud-check-enabled': boolean;
  'notification-channel': NotificationChannel;
  'max-retry-attempts': number;
}

export type FlagKey = keyof FlagValues;

type Definition<T> = {
  description: string;
  /** Served whenever the provider is unavailable or returns something invalid. */
  defaultValue: T;
} & ([T] extends [number]
  ? { type: 'number'; min: number; max: number; integer?: boolean }
  : [T] extends [boolean]
    ? { type: 'boolean' }
    : { type: 'string'; allowed: readonly T[] });

/**
 * Every flag with its type, bounds and *safe default*. The defaults are what the
 * pipeline runs with when LaunchDarkly is unreachable or not configured, so they
 * must always describe normal, healthy behaviour.
 */
export const FLAGS: { [K in FlagKey]: Definition<FlagValues[K]> } = {
  'payment-failure-rate': {
    type: 'number',
    min: 0,
    max: 1,
    defaultValue: 0,
    description:
      'Chaos: probability (0-1) that a simulated payment gateway call throws a transient error',
  },
  'payment-consumer-enabled': {
    type: 'boolean',
    defaultValue: true,
    description:
      'Kill switch: false pauses the payment-service consumer (lag builds up), true resumes it',
  },
  'fraud-check-enabled': {
    type: 'boolean',
    defaultValue: false,
    description: 'Adds a fraud-check step before charging (tier limits, unknown country)',
  },
  'notification-channel': {
    type: 'string',
    allowed: NOTIFICATION_CHANNELS,
    defaultValue: 'email',
    description: 'Channel notification-service uses for customer notifications',
  },
  'max-retry-attempts': {
    type: 'number',
    min: 0,
    max: 10,
    integer: true,
    defaultValue: 3,
    description:
      'Retries (after the first attempt) before the consumer wrapper dead-letters a message',
  },
};

export const FLAG_KEYS = Object.keys(FLAGS) as FlagKey[];

export function isFlagKey(key: string): key is FlagKey {
  return Object.hasOwn(FLAGS, key);
}

export type Sanitized<K extends FlagKey> = { value: FlagValues[K]; problem?: string };

/**
 * Validates a raw value from any provider. Wrong types or out-of-range values fall
 * back to the flag's safe default (and report why); strings like "0.5" or "true"
 * from environment variables are coerced.
 */
export function sanitize<K extends FlagKey>(key: K, raw: unknown): Sanitized<K> {
  const def = FLAGS[key] as Definition<FlagValues[K]> & { type: string };
  const fallback = (problem: string): Sanitized<K> => ({ value: def.defaultValue, problem });

  switch (def.type) {
    case 'number': {
      const { min, max, integer } = def as unknown as {
        min: number;
        max: number;
        integer?: boolean;
      };
      const n = typeof raw === 'string' && raw.trim() !== '' ? Number(raw) : raw;
      if (typeof n !== 'number' || !Number.isFinite(n))
        return fallback(`expected a number, got ${JSON.stringify(raw)}`);
      if (integer && !Number.isInteger(n)) return fallback(`expected an integer, got ${n}`);
      if (n < min || n > max) return fallback(`${n} is outside [${min}, ${max}]`);
      return { value: n as FlagValues[K] };
    }
    case 'boolean': {
      const b = raw === 'true' ? true : raw === 'false' ? false : raw;
      if (typeof b !== 'boolean') return fallback(`expected a boolean, got ${JSON.stringify(raw)}`);
      return { value: b as FlagValues[K] };
    }
    default: {
      const { allowed } = def as unknown as { allowed: readonly string[] };
      if (typeof raw !== 'string' || !allowed.includes(raw)) {
        return fallback(`expected one of ${allowed.join(', ')}, got ${JSON.stringify(raw)}`);
      }
      return { value: raw as FlagValues[K] };
    }
  }
}

export function defaultValues(): FlagValues {
  return Object.fromEntries(
    FLAG_KEYS.map((k) => [k, FLAGS[k].defaultValue]),
  ) as unknown as FlagValues;
}
