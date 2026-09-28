export interface RetryPolicy {
  /** Retries after the first attempt. 3 => up to 4 attempts in total. */
  maxRetries: number;
  initialDelayMs: number;
  maxDelayMs: number;
  /** Exponential growth factor between attempts. */
  multiplier: number;
  /** Random +/- spread applied to each delay (0.2 => +/-20%) to avoid thundering herds. */
  jitter: number;
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  maxRetries: 3,
  initialDelayMs: 200,
  maxDelayMs: 5000,
  multiplier: 2,
  jitter: 0.2,
};

/**
 * Delay before retry number `retry` (1-based):
 * min(maxDelay, initialDelay * multiplier^(retry-1)) with +/- jitter.
 */
export function backoffDelay(
  retry: number,
  policy: RetryPolicy,
  random: () => number = Math.random,
): number {
  const exponential = policy.initialDelayMs * policy.multiplier ** Math.max(0, retry - 1);
  const capped = Math.min(policy.maxDelayMs, exponential);
  const spread = capped * policy.jitter * (random() * 2 - 1);
  return Math.max(0, Math.round(capped + spread));
}

/** Promise-based sleep that rejects early if the signal aborts (used on shutdown). */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new Error('Aborted'));
      return;
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(new Error('Aborted'));
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}
