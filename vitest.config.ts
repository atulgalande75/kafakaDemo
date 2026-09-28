import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

const src = (pkg: string) =>
  fileURLToPath(new URL(`./packages/${pkg}/src/index.ts`, import.meta.url));

export default defineConfig({
  resolve: {
    // Tests run against workspace sources directly - no build step required.
    alias: {
      '@orderflow/contracts': src('contracts'),
      '@orderflow/kafka-utils': src('kafka-utils'),
    },
  },
  test: {
    include: ['{packages,services,tools}/*/src/**/*.test.ts'],
    environment: 'node',
  },
});
