import { InvalidEventError } from '@orderflow/contracts';

/**
 * Throw from a handler when retrying cannot possibly help (bad data, violated
 * invariant). The message is sent to the DLQ immediately.
 */
export class NonRetryableError extends Error {
  override readonly name = 'NonRetryableError';
}

export function isRetryable(err: unknown): boolean {
  return !(err instanceof NonRetryableError || err instanceof InvalidEventError);
}

export function toError(err: unknown): Error {
  return err instanceof Error ? err : new Error(String(err));
}
