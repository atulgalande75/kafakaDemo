// Runs before `npm run dev`: installs dependencies on a fresh clone so that
// `docker compose up` + `npm run dev` is all you need. Uses only Node built-ins.
import { existsSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

if (!existsSync(new URL('../node_modules/.package-lock.json', import.meta.url))) {
  console.log('[orderflow] node_modules missing - running `npm install` first...');
  const result = spawnSync('npm', ['install', '--no-audit', '--no-fund'], {
    stdio: 'inherit',
    shell: process.platform === 'win32',
  });
  process.exit(result.status ?? 1);
}
