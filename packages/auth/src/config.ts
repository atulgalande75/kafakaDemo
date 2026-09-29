export interface AuthConfig {
  issuer: string;
  audience: string;
  jwksUri: string;
  tokenUrl: string;
}

const env = (name: string, fallback: string) => process.env[name]?.trim() || fallback;

/** Keycloak (docker-compose) defaults; override with AUTH_* environment variables. */
export function authConfigFromEnv(): AuthConfig {
  const issuer = env('AUTH_ISSUER', 'http://localhost:8081/realms/orderflow').replace(/\/$/, '');
  return {
    issuer,
    audience: env('AUTH_AUDIENCE', 'order-service'),
    jwksUri: env('AUTH_JWKS_URI', `${issuer}/protocol/openid-connect/certs`),
    tokenUrl: env('AUTH_TOKEN_URL', `${issuer}/protocol/openid-connect/token`),
  };
}
