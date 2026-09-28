// Starts every service in watch mode with prefixed, colored output.
//
//   npm run dev                          # all services
//   npm run dev -- --only payment        # just payment-service
//   npm run dev -- --skip payment        # everything except payment-service
//
// A ./.env file (see .env.example) is loaded if present and inherited by all services.
import { existsSync } from 'node:fs';
import { parseArgs } from 'node:util';
import concurrently from 'concurrently';

const SERVICES = [
  { name: 'order', workspace: '@orderflow/order-service', color: 'cyan' },
  { name: 'payment', workspace: '@orderflow/payment-service', color: 'magenta' },
  { name: 'inventory', workspace: '@orderflow/inventory-service', color: 'yellow' },
  { name: 'notification', workspace: '@orderflow/notification-service', color: 'green' },
];

const { values } = parseArgs({
  options: {
    only: { type: 'string', multiple: true, default: [] },
    skip: { type: 'string', multiple: true, default: [] },
  },
});

const split = (list) =>
  list.flatMap((v) => v.split(',')).map((v) => v.trim().replace(/-service$/, ''));
const only = split(values.only);
const skip = split(values.skip);

const unknown = [...only, ...skip].filter((n) => !SERVICES.some((s) => s.name === n));
if (unknown.length > 0) {
  console.error(
    `Unknown service(s): ${unknown.join(', ')}. Valid: ${SERVICES.map((s) => s.name).join(', ')}`,
  );
  process.exit(1);
}

const selected = SERVICES.filter(
  (s) => (only.length === 0 || only.includes(s.name)) && !skip.includes(s.name),
);

const envFile = new URL('../.env', import.meta.url);
if (existsSync(envFile)) {
  process.loadEnvFile(envFile);
  console.log('[orderflow] loaded .env');
}

const { result } = concurrently(
  selected.map((s) => ({
    command: `npm run dev --silent -w ${s.workspace}`,
    name: s.name,
    prefixColor: s.color,
  })),
  { prefix: 'name', padPrefix: true, killOthersOn: [], handleInput: false },
);

result.catch(() => process.exit(1));
