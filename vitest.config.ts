import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    // Resolve workspace packages to their TypeScript sources (see the "@orderflow/source"
    // export condition in each package.json) - no build step needed for tests.
    conditions: ['@orderflow/source'],
  },
  ssr: {
    resolve: { conditions: ['@orderflow/source'], externalConditions: ['@orderflow/source'] },
  },
  test: {
    include: ['{packages,services,tools}/*/src/**/*.test.ts'],
    environment: 'node',
    // Starting the in-process Postgres (PGlite) can take a while when many workers boot at once.
    hookTimeout: 60_000,
  },
});
