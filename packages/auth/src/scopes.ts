/** OAuth scopes understood by the services (defined as client scopes in the Keycloak realm). */
export const Scopes = {
  OrdersRead: 'orders:read',
  OrdersWrite: 'orders:write',
  /** inventory-service: view stock levels. */
  InventoryRead: 'inventory:read',
  /** inventory-service: change stock levels (restock, shrinkage, corrections). */
  InventoryWrite: 'inventory:write',
  /** gateway-service: may open the real-time stream (what it contains depends on the data scopes). */
  StreamRead: 'stream:read',
  Admin: 'admin',
} as const;
