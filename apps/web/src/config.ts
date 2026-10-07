/** Where the browser talks to. Defaults match docker-compose; override with VITE_* variables. */
export const config = {
  oidc: {
    authority: import.meta.env.VITE_OIDC_AUTHORITY ?? 'http://localhost:8081/realms/orderflow',
    clientId: import.meta.env.VITE_OIDC_CLIENT_ID ?? 'orderflow-web',
    scope:
      import.meta.env.VITE_OIDC_SCOPE ??
      'openid profile orders:read orders:write inventory:read inventory:write stream:read',
  },
  /** Each service is reached through the dev server's proxy (see vite.config.ts). */
  apiBase: {
    'order-service': import.meta.env.VITE_ORDER_API ?? '/api/order-service',
    'inventory-service': import.meta.env.VITE_INVENTORY_API ?? '/api/inventory-service',
    'gateway-service': import.meta.env.VITE_GATEWAY_API ?? '/api/gateway-service',
  },
} as const;

export type ServiceName = keyof typeof config.apiBase;
