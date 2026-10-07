import { afterEach, describe, expect, it, vi } from 'vitest';
import { ApiError, api, describeError } from './client';

const reply = (status: number, body?: unknown) =>
  new Response(body === undefined ? null : JSON.stringify(body), { status });

afterEach(() => vi.unstubAllGlobals());

describe('api', () => {
  it('sends the bearer token and a JSON body to the right service', async () => {
    const fetchMock = vi.fn().mockResolvedValue(reply(202, { id: 'o-1' }));
    vi.stubGlobal('fetch', fetchMock);

    const result = await api(() => Promise.resolve('tok'), 'order-service', '/orders', {
      method: 'POST',
      body: { a: 1 },
    });

    expect(result).toEqual({ id: 'o-1' });
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe('/api/order-service/orders');
    expect(init.method).toBe('POST');
    expect(init.body).toBe('{"a":1}');
    expect(init.headers).toMatchObject({
      authorization: 'Bearer tok',
      'content-type': 'application/json',
    });
  });

  it('does not send a content type without a body', async () => {
    const fetchMock = vi.fn().mockResolvedValue(reply(200, []));
    vi.stubGlobal('fetch', fetchMock);
    await api(() => Promise.resolve('t'), 'inventory-service', '/inventory');
    const [, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(init.headers).not.toHaveProperty('content-type');
  });

  it('throws an ApiError carrying the status and the service message', async () => {
    vi.stubGlobal(
      'fetch',
      vi
        .fn()
        .mockResolvedValue(
          reply(409, { error: 'insufficient_stock', message: 'only 0 available' }),
        ),
    );
    const error = await api(() => Promise.resolve('t'), 'inventory-service', '/x').catch(
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(ApiError);
    expect(error).toMatchObject({ status: 409, message: 'only 0 available' });
  });
});

describe('describeError', () => {
  it('lists field errors from validation responses', () => {
    expect(
      describeError(400, { error: 'Invalid adjustment', details: { delta: ['must not be 0'] } }),
    ).toBe('Invalid adjustment (delta: must not be 0)');
  });

  it('explains auth failures in plain words', () => {
    expect(describeError(401, undefined)).toMatch(/sign in again/i);
    expect(describeError(403, {})).toMatch(/not allowed/i);
  });

  it('falls back to the status code', () => {
    expect(describeError(502, 'Bad gateway html')).toBe('Request failed (502)');
  });
});
