/// <reference types="vitest/config" />
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

const target = (envName: string, fallback: string) => process.env[envName] || fallback;

/** Forwards `/api/<service>/...` to that service, so the browser only ever talks to one origin (no CORS). */
const proxyTo = (service: string, url: string) => ({
  [`/api/${service}`]: {
    target: url,
    changeOrigin: true,
    rewrite: (path: string) => path.replace(new RegExp(`^/api/${service}`), ''),
  },
});

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true, // Keycloak only accepts http://localhost:5173 as a redirect target
    proxy: {
      ...proxyTo('order-service', target('ORDER_SERVICE_URL', 'http://localhost:3000')),
      ...proxyTo('inventory-service', target('INVENTORY_SERVICE_URL', 'http://localhost:3001')),
      ...proxyTo('gateway-service', target('GATEWAY_SERVICE_URL', 'http://localhost:3002')),
    },
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.ts'],
    include: ['src/**/*.test.{ts,tsx}'],
    css: false,
  },
});
