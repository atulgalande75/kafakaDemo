// Starts every service in watch mode with prefixed, colored output.
//
//   npm run dev                          # all services
//   npm run dev -- --only payment        # just payment-service
//   npm run dev -- --skip payment        # everything except payment-service
//
// A ./.env file (see .env.example) is loaded if present and inherited by all services.
import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

const SERVICES = [
  { name: 'order', color: 36 },
  { name: 'payment', color: 35 },
  { name: 'inventory', color: 33 },
  { name: 'notification', color: 32 },
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
  process.loadEnvFile(fileURLToPath(envFile));
  console.log('[orderflow] loaded .env');
}

const tsx = fileURLToPath(new URL('../node_modules/tsx/dist/cli.mjs', import.meta.url));
const width = Math.max(...selected.map((s) => s.name.length));
const useColor = process.env.NO_COLOR === undefined;
const children = new Set();

for (const service of selected) {
  const label = `[${service.name.padEnd(width)}]`;
  const prefix = useColor ? `\x1b[${service.color}m${label}\x1b[0m ` : `${label} `;
  // Spawned directly (no npm/sh layers) so a single Ctrl+C reaches every process once
  // and each service can disconnect its consumer cleanly before exiting.
  const child = spawn(
    process.execPath,
    [tsx, 'watch', '--clear-screen=false', '--conditions=@orderflow/source', 'src/index.ts'],
    {
      cwd: fileURLToPath(new URL(`../services/${service.name}-service`, import.meta.url)),
      stdio: ['ignore', 'pipe', 'pipe'],
    },
  );
  children.add(child);
  for (const stream of [child.stdout, child.stderr]) {
    createInterface({ input: stream }).on('line', (line) => console.log(prefix + line));
  }
  child.on('exit', (code, signal) => {
    children.delete(child);
    console.log(`${prefix}exited (${signal ?? code})`);
    if (children.size === 0) process.exit(0);
  });
}

// Ctrl+C in a terminal already signals every process in the foreground group, so
// just wait for the services to finish. Anything else (e.g. `kill <pid>`) is forwarded.
process.on('SIGINT', () => {});
process.on('SIGTERM', () => {
  for (const child of children) child.kill('SIGTERM');
});
