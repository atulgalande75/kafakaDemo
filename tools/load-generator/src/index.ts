#!/usr/bin/env node
/**
 * Load generator for the order pipeline.
 *
 *   npm run load                                   # 20 orders at 5/s
 *   npm run load -- -n 200 -r 50 --scenario mixed --wait
 *   npm run load -- -n 5 --duplicate --wait        # re-publish each orders.created (same eventId)
 *   npm run load -- --poison 5                     # write 5 poison messages to orders.created
 */
import { setTimeout as delay } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import { Topics } from '@orderflow/contracts';
import {
  createEventProducer,
  createKafka,
  createLogger,
  ensureTopics,
} from '@orderflow/kafka-utils';
import { poisonMessages } from './poison.js';
import { SCENARIOS, randomOrder, type Scenario } from './scenarios.js';
import { summarize, type SettledOrder } from './stats.js';

const USAGE = `Usage: npm run load -- [options]

  -n, --count <n>        number of orders to create (default 20)
  -r, --rate <n>         orders per second (default 5)
  -s, --scenario <name>  ${SCENARIOS.join(' | ')} (default happy)
  -w, --wait             wait for orders to settle and print a summary
      --timeout <sec>    max seconds to wait with --wait (default 60)
      --duplicate        re-publish every orders.created event (duplicate delivery demo)
      --poison <n>       publish <n> malformed messages straight to Kafka and exit
      --url <url>        order-service base URL (default http://localhost:3000)
  -h, --help`;

// Keep CLI output focused on results; set LOG_LEVEL=info to see Kafka client logs.
process.env.LOG_LEVEL ??= 'warn';

const { values } = parseArgs({
  options: {
    count: { type: 'string', short: 'n', default: '20' },
    rate: { type: 'string', short: 'r', default: '5' },
    scenario: { type: 'string', short: 's', default: 'happy' },
    wait: { type: 'boolean', short: 'w', default: false },
    timeout: { type: 'string', default: '60' },
    duplicate: { type: 'boolean', default: false },
    poison: { type: 'string' },
    url: { type: 'string', default: process.env.ORDER_SERVICE_URL ?? 'http://localhost:3000' },
    help: { type: 'boolean', short: 'h', default: false },
  },
});

function positiveInt(name: string, raw: string): number {
  const n = Number(raw);
  if (!Number.isInteger(n) || n < 1) throw new Error(`--${name} must be a positive integer`);
  return n;
}

async function sendPoison(count: number) {
  const logger = createLogger('load-generator');
  const kafka = createKafka({ clientId: 'load-generator', logger });
  await ensureTopics(kafka, logger);
  const producer = createEventProducer(kafka, logger);
  await producer.connect();
  try {
    await producer.sendRaw(Topics.OrdersCreated, poisonMessages(count));
    console.log(`☠️  published ${count} poison message(s) to ${Topics.OrdersCreated}`);
    console.log(`   expect them in ${Topics.OrdersCreated}.dlq (once per consuming group)`);
  } finally {
    await producer.disconnect();
  }
}

interface CreatedOrder extends SettledOrder {
  id: string;
}

async function http<T>(method: string, url: string, body?: unknown): Promise<T> {
  const res = await fetch(url, {
    method,
    headers: body === undefined ? {} : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${method} ${url} -> ${res.status} ${await res.text()}`);
  return (await res.json()) as T;
}

async function createOrders(opts: {
  url: string;
  count: number;
  rate: number;
  scenario: Scenario;
  duplicate: boolean;
}): Promise<CreatedOrder[]> {
  const intervalMs = 1000 / opts.rate;
  const started = Date.now();
  const pending: Array<Promise<CreatedOrder | undefined>> = [];
  let failed = 0;

  for (let i = 0; i < opts.count; i++) {
    pending.push(
      (async () => {
        try {
          const order = await http<CreatedOrder>(
            'POST',
            `${opts.url}/orders`,
            randomOrder(opts.scenario),
          );
          if (opts.duplicate) {
            await http('POST', `${opts.url}/orders/${order.id}/republish`);
          }
          return order;
        } catch (err) {
          failed++;
          console.error(`  ✗ ${(err as Error).message}`);
          return undefined;
        }
      })(),
    );
    // Pace requests to the requested rate (absolute schedule, so drift doesn't accumulate).
    const nextAt = started + (i + 1) * intervalMs;
    await delay(Math.max(0, nextAt - Date.now()));
  }

  const created = (await Promise.all(pending)).filter((o): o is CreatedOrder => o !== undefined);
  const seconds = (Date.now() - started) / 1000;
  console.log(
    `📦 created ${created.length}/${opts.count} orders in ${seconds.toFixed(1)}s` +
      (failed ? ` (${failed} failed)` : '') +
      (opts.duplicate ? ` - each orders.created event published twice` : ''),
  );
  return created;
}

async function waitForSettled(url: string, orders: CreatedOrder[], timeoutSec: number) {
  const deadline = Date.now() + timeoutSec * 1000;
  let latest = orders;
  process.stdout.write('⏳ waiting for orders to settle');
  while (Date.now() < deadline) {
    latest = await Promise.all(
      orders.map((o) => http<CreatedOrder>('GET', `${url}/orders/${o.id}`)),
    );
    if (latest.every((o) => o.status !== 'PENDING')) break;
    process.stdout.write('.');
    await delay(500);
  }
  process.stdout.write('\n');

  const summary = summarize(latest);
  console.log('\n📊 Summary');
  console.table(summary.byStatus);
  if (Object.keys(summary.cancelReasons).length > 0) {
    console.log('Cancellation reasons:');
    console.table(summary.cancelReasons);
  }
  console.log(
    `End-to-end latency (created -> final): p50 ${summary.latencyMs.p50}ms, ` +
      `p95 ${summary.latencyMs.p95}ms, max ${summary.latencyMs.max}ms`,
  );
  const stuck = summary.byStatus.PENDING ?? 0;
  if (stuck > 0) {
    console.log(
      `⚠️  ${stuck} order(s) still PENDING after ${timeoutSec}s - is every service running?`,
    );
    process.exitCode = 2;
  }
}

async function main() {
  if (values.help) {
    console.log(USAGE);
    return;
  }
  if (values.poison !== undefined) {
    await sendPoison(positiveInt('poison', values.poison));
    return;
  }
  const scenario = values.scenario as Scenario;
  if (!SCENARIOS.includes(scenario))
    throw new Error(`--scenario must be one of ${SCENARIOS.join(', ')}`);

  const url = values.url.replace(/\/$/, '');
  const orders = await createOrders({
    url,
    count: positiveInt('count', values.count),
    rate: positiveInt('rate', values.rate),
    scenario,
    duplicate: values.duplicate,
  });
  if (values.wait && orders.length > 0) {
    await waitForSettled(url, orders, positiveInt('timeout', values.timeout));
  }
}

main().catch((err: unknown) => {
  const cause = (err as { cause?: { code?: string } }).cause;
  if (cause?.code === 'ECONNREFUSED') {
    console.error(`Cannot reach order-service at ${values.url} - is \`npm run dev\` running?`);
  } else {
    console.error(err);
  }
  process.exit(1);
});
