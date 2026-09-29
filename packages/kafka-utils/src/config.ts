import type { RetryPolicy } from './backoff.js';

export function envString(name: string, fallback: string): string {
  const value = process.env[name]?.trim();
  return value ? value : fallback;
}

export function envNumber(
  name: string,
  fallback: number,
  { min = -Infinity, max = Infinity }: { min?: number; max?: number } = {},
): number {
  const raw = process.env[name]?.trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < min || value > max) {
    throw new Error(`Environment variable ${name}="${raw}" must be a number in [${min}, ${max}]`);
  }
  return value;
}

/** A probability between 0 and 1 (e.g. PAYMENT_FAILURE_RATE). */
export function envRate(name: string, fallback: number): number {
  return envNumber(name, fallback, { min: 0, max: 1 });
}

export function brokersFromEnv(): string[] {
  return envString('KAFKA_BROKERS', 'localhost:9092')
    .split(',')
    .map((b) => b.trim())
    .filter(Boolean);
}

export function retryPolicyFromEnv(): RetryPolicy {
  return {
    // Per-message retry count comes from the max-retry-attempts feature flag.
    maxRetries: 3,
    initialDelayMs: envNumber('CONSUMER_INITIAL_RETRY_MS', 200, { min: 0 }),
    maxDelayMs: envNumber('CONSUMER_MAX_RETRY_MS', 5000, { min: 0 }),
    multiplier: 2,
    jitter: 0.2,
  };
}
