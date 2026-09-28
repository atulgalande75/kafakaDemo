/** OAuth scopes understood by order-service (defined as client scopes in the Keycloak realm). */
export const Scopes = {
  OrdersRead: 'orders:read',
  OrdersWrite: 'orders:write',
  Admin: 'admin',
} as const;
