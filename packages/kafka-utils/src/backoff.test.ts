import { describe, expect, it } from 'vitest';
import { backoffDelay, sleep, type RetryPolicy } from './backoff.js';
import { InMemoryIdempotencyStore } from './idempotency.js';

const policy: RetryPolicy = {
  maxRetries: 5,
  initialDelayMs: 100,
  maxDelayMs: 1000,
  multiplier: 2,
  jitter: 0.2,
};
const noJitter = () => 0.5; // random()*2-1 === 0

describe('backoffDelay', () => {
  it('grows exponentially and is capped', () => {
    expect([1, 2, 3, 4, 5, 6].map((r) => backoffDelay(r, policy, noJitter))).toEqual([
      100, 200, 400, 800, 1000, 1000,
    ]);
  });

  it('applies bounded jitter', () => {
    expect(backoffDelay(2, policy, () => 0)).toBe(160);
    expect(backoffDelay(2, policy, () => 1)).toBe(240);
  });
});

describe('sleep', () => {
  it('rejects when aborted', async () => {
    const ac = new AbortController();
    const pending = sleep(10_000, ac.signal);
    ac.abort();
    await expect(pending).rejects.toThrow('Aborted');
  });
});

describe('InMemoryIdempotencyStore', () => {
  it('remembers event ids and evicts the oldest beyond capacity', async () => {
    const store = new InMemoryIdempotencyStore(2);
    await store.add('a');
    await store.add('b');
    expect(await store.has('a')).toBe(true);
    await store.add('c');
    expect(await store.has('a')).toBe(false);
    expect(await store.has('b')).toBe(true);
    expect(await store.has('c')).toBe(true);
    expect(store.size).toBe(2);
  });
});
