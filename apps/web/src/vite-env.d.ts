/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_OIDC_AUTHORITY?: string;
  readonly VITE_OIDC_CLIENT_ID?: string;
  readonly VITE_OIDC_SCOPE?: string;
  readonly VITE_ORDER_API?: string;
  readonly VITE_INVENTORY_API?: string;
  readonly VITE_GATEWAY_API?: string;
}
