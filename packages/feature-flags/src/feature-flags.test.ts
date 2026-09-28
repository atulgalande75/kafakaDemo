import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { TestData } from '@launchdarkly/node-server-sdk/integrations';
import { pino } from 'pino';
import { afterEach, describe, expect, it } from 'vitest';
import { orderContext, serviceContext, toLdContext } from './context.js';
import { defaultValues, sanitize } from './definitions.js';
import { watchFlag, type FeatureFlags } from './feature-flags.js';
import { LaunchDarklyFeatureFlags } from './launchdarkly.js';
import { LocalFeatureFlags } from './local.js';

function capturingLogger() {
  const lines: Array<Record<string, unknown>> = [];
  const logger = pino(
    { level: 'debug' },
    { write: (msg: string) => void lines.push(JSON.parse(msg) as Record<string, unknown>) },
  );
  return { logger, lines, messages: () => lines.map((l) => l.msg) };
}

const order = orderContext({ orderId: 'o-1', customerTier: 'gold', country: 'DE' });
const toClose: FeatureFlags[] = [];
afterEach(async () => {
  await Promise.all(toClose.splice(0).map((f) => f.close()));
});

describe('sanitize', () => {
  it('accepts valid values and coerces env-style strings', () => {
    expect(sanitize('payment-failure-rate', 0.25).value).toBe(0.25);
    expect(sanitize('payment-failure-rate', '0.5').value).toBe(0.5);
    expect(sanitize('payment-consumer-enabled', 'false').value).toBe(false);
    expect(sanitize('notification-channel', 'sms').value).toBe('sms');
    expect(sanitize('max-retry-attempts', '5').value).toBe(5);
  });

  it('falls back to the safe default for invalid values', () => {
    expect(sanitize('payment-failure-rate', 7)).toMatchObject({
      value: 0,
      problem: expect.stringContaining('outside') as unknown,
    });
    expect(sanitize('payment-consumer-enabled', 'yes')).toMatchObject({ value: true });
    expect(sanitize('notification-channel', 'carrier-pigeon')).toMatchObject({ value: 'email' });
    expect(sanitize('max-retry-attempts', 2.5)).toMatchObject({ value: 3 });
    expect(sanitize('max-retry-attempts', null)).toMatchObject({ value: 3 });
  });

  it('has healthy defaults', () => {
    expect(defaultValues()).toEqual({
      'payment-failure-rate': 0,
      'payment-consumer-enabled': true,
      'fraud-check-enabled': false,
      'notification-channel': 'email',
      'max-retry-attempts': 3,
    });
  });
});

describe('toLdContext', () => {
  it('builds order and service contexts', () => {
    expect(toLdContext(order)).toEqual({
      kind: 'order',
      key: 'o-1',
      customerTier: 'gold',
      country: 'DE',
    });
    expect(toLdContext(orderContext({ orderId: 'o-2' }))).toEqual({ kind: 'order', key: 'o-2' });
    expect(toLdContext(serviceContext('payment-service'))).toEqual({
      kind: 'service',
      key: 'payment-service',
    });
  });
});

describe('LocalFeatureFlags', () => {
  it('serves safe defaults with no configuration', async () => {
    const flags = new LocalFeatureFlags({ logger: pino({ level: 'silent' }), env: {} });
    expect(await flags.get('payment-consumer-enabled', order)).toBe(true);
    expect(await flags.get('max-retry-attempts', order)).toBe(3);
  });

  it('layers file < values < FLAG_* env vars', async () => {
    const file = join(mkdtempSync(join(tmpdir(), 'flags-')), 'flags.json');
    writeFileSync(
      file,
      JSON.stringify({
        'payment-failure-rate': 0.1,
        'notification-channel': 'sms',
        'max-retry-attempts': 1,
      }),
    );
    const flags = new LocalFeatureFlags({
      logger: pino({ level: 'silent' }),
      file,
      watchIntervalMs: 0,
      values: { 'notification-channel': 'push' },
      env: { FLAG_MAX_RETRY_ATTEMPTS: '5' },
    });
    expect(await flags.get('payment-failure-rate', order)).toBe(0.1);
    expect(await flags.get('notification-channel', order)).toBe('push');
    expect(await flags.get('max-retry-attempts', order)).toBe(5);
  });

  it('warns and uses the default for invalid or unknown entries', async () => {
    const { logger, lines } = capturingLogger();
    const flags = new LocalFeatureFlags({
      logger,
      env: {},
      values: { 'payment-failure-rate': 3, ['no-such-flag' as 'fraud-check-enabled']: true },
    });
    expect(await flags.get('payment-failure-rate', order)).toBe(0);
    expect(lines.map((l) => l.msg)).toEqual(
      expect.arrayContaining([
        'invalid flag value, using safe default',
        'unknown feature flag ignored',
      ]),
    );
  });

  it('logs and announces changes when the file is edited', async () => {
    const { logger, lines } = capturingLogger();
    const file = join(mkdtempSync(join(tmpdir(), 'flags-')), 'flags.json');
    writeFileSync(file, JSON.stringify({ 'payment-consumer-enabled': true }));
    const flags = new LocalFeatureFlags({ logger, file, watchIntervalMs: 0, env: {} });
    const changed: string[] = [];
    flags.onChange((key) => changed.push(key));

    writeFileSync(file, JSON.stringify({ 'payment-consumer-enabled': false }));
    flags.reload();

    expect(await flags.get('payment-consumer-enabled', order)).toBe(false);
    expect(changed).toEqual(['payment-consumer-enabled']);
    expect(lines.find((l) => l.msg === 'feature flag changed')).toMatchObject({
      flag: 'payment-consumer-enabled',
      from: true,
      to: false,
    });
  });

  it('keeps previous values when the file becomes invalid JSON', async () => {
    const file = join(mkdtempSync(join(tmpdir(), 'flags-')), 'flags.json');
    writeFileSync(file, '{ not json');
    const flags = new LocalFeatureFlags({
      logger: pino({ level: 'silent' }),
      file,
      watchIntervalMs: 0,
      env: {},
    });
    expect(await flags.get('fraud-check-enabled', order)).toBe(false);
  });
});

describe('watchFlag', () => {
  it('applies the current value and then only real changes', async () => {
    const flags = new LocalFeatureFlags({ logger: pino({ level: 'silent' }), env: {} });
    const seen: Array<[boolean, boolean | undefined]> = [];
    await watchFlag(
      flags,
      'payment-consumer-enabled',
      serviceContext('payment-service'),
      (v, prev) => seen.push([v, prev]),
    );
    flags.set('payment-consumer-enabled', false);
    flags.set('fraud-check-enabled', true); // other flag - ignored
    flags.set('payment-consumer-enabled', true);
    await new Promise((r) => setImmediate(r));
    expect(seen).toEqual([
      [true, undefined],
      [false, true],
      [true, false],
    ]);
  });
});

describe('LaunchDarklyFeatureFlags', () => {
  async function withTestData(setup: (td: TestData) => Promise<unknown>) {
    const td = new TestData();
    await setup(td);
    const log = capturingLogger();
    const flags = await new LaunchDarklyFeatureFlags({
      sdkKey: 'test-sdk-key',
      logger: log.logger,
      service: 'payment-service',
      clientOptions: {
        updateProcessor: td.getFactory(),
        sendEvents: false,
        diagnosticOptOut: true,
      },
    }).start();
    toClose.push(flags);
    return { td, flags, ...log };
  }

  it('evaluates flags with order context targeting', async () => {
    const { flags } = await withTestData((td) =>
      td.update(
        td
          .flag('fraud-check-enabled')
          .booleanFlag()
          .fallthroughVariation(false)
          .ifMatch('order', 'customerTier', 'standard')
          .thenReturn(true),
      ),
    );
    expect(
      await flags.get(
        'fraud-check-enabled',
        orderContext({ orderId: 'a', customerTier: 'standard' }),
      ),
    ).toBe(true);
    expect(await flags.get('fraud-check-enabled', order)).toBe(false);
  });

  it('serves safe defaults for flags LaunchDarkly does not know', async () => {
    const { flags } = await withTestData(() => Promise.resolve());
    expect(await flags.get('payment-consumer-enabled', order)).toBe(true);
    expect(await flags.get('notification-channel', order)).toBe('email');
  });

  it('replaces invalid values from LaunchDarkly with the safe default', async () => {
    const { flags } = await withTestData((td) =>
      td.update(td.flag('payment-failure-rate').valueForAll(42)),
    );
    expect(await flags.get('payment-failure-rate', order)).toBe(0);
  });

  it('logs and announces flag changes', async () => {
    const { td, flags, lines } = await withTestData((td) =>
      td.update(td.flag('payment-consumer-enabled').booleanFlag().variationForAll(true)),
    );
    const changed: string[] = [];
    flags.onChange((key) => changed.push(key));

    await td.update(td.flag('payment-consumer-enabled').booleanFlag().variationForAll(false));
    await new Promise((r) => setTimeout(r, 20));

    expect(changed).toEqual(['payment-consumer-enabled']);
    expect(await flags.get('payment-consumer-enabled', serviceContext('payment-service'))).toBe(
      false,
    );
    expect(lines.find((l) => l.msg === 'feature flag changed')).toMatchObject({
      flag: 'payment-consumer-enabled',
      from: true,
      to: false,
    });
  });

  it('keeps working with safe defaults when LaunchDarkly is unreachable', async () => {
    const log = capturingLogger();
    const flags = await new LaunchDarklyFeatureFlags({
      sdkKey: 'sdk-unreachable',
      logger: log.logger,
      service: 'payment-service',
      initTimeoutSec: 0.5,
      clientOptions: {
        baseUri: 'http://127.0.0.1:9',
        streamUri: 'http://127.0.0.1:9',
        eventsUri: 'http://127.0.0.1:9',
        sendEvents: false,
        diagnosticOptOut: true,
      },
    }).start();
    toClose.push(flags);

    expect(log.messages()).toContain(
      'LaunchDarkly unavailable - serving safe defaults until it connects',
    );
    expect(await flags.get('payment-failure-rate', order)).toBe(0);
    expect(await flags.get('payment-consumer-enabled', order)).toBe(true);
    expect(await flags.get('max-retry-attempts', order)).toBe(3);
  });
});
