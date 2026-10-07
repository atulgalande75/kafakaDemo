import { config, type ServiceName } from '../config';

export class ApiError extends Error {
  override readonly name = 'ApiError';
  constructor(
    readonly status: number,
    message: string,
    readonly body?: unknown,
  ) {
    super(message);
  }
}

export type GetToken = () => Promise<string>;

export interface RequestOptions {
  method?: 'GET' | 'POST';
  body?: unknown;
  signal?: AbortSignal;
}

/** Calls one of the backend services with the user's access token. Throws {@link ApiError} on non-2xx. */
export async function api<T>(
  getToken: GetToken,
  service: ServiceName,
  path: string,
  { method = 'GET', body, signal }: RequestOptions = {},
): Promise<T> {
  const response = await fetch(`${config.apiBase[service]}${path}`, {
    method,
    signal,
    headers: {
      authorization: `Bearer ${await getToken()}`,
      ...(body !== undefined && { 'content-type': 'application/json' }),
    },
    ...(body !== undefined && { body: JSON.stringify(body) }),
  });

  const text = await response.text();
  let payload: unknown;
  try {
    payload = text ? JSON.parse(text) : undefined;
  } catch {
    payload = text;
  }
  if (!response.ok)
    throw new ApiError(response.status, describeError(response.status, payload), payload);
  return payload as T;
}

/** A readable message from the services' error bodies ({ error, message, details }). */
export function describeError(status: number, payload: unknown): string {
  if (payload && typeof payload === 'object') {
    const { message, error, details } = payload as {
      message?: unknown;
      error?: unknown;
      details?: unknown;
    };
    const text = [message, error].find((v): v is string => typeof v === 'string' && v.length > 0);
    if (details && typeof details === 'object') {
      const fields = Object.entries(details as Record<string, unknown>)
        .map(([field, problems]) => `${field}: ${[problems].flat().join(', ')}`)
        .join('; ');
      return text ? `${text} (${fields})` : fields;
    }
    if (text) return text;
  }
  if (status === 401) return 'Your session has expired. Please sign in again.';
  if (status === 403) return 'You are not allowed to do that.';
  return `Request failed (${status})`;
}
