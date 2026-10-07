export interface AuthConfig {
  issuer: string;
  audience: string;
  jwksUri: string;
  tokenUrl: string;
}

const env = (name: string, fallback: string) => process.env[name]?.trim() || fallback;

export interface AuthConfigOptions {
  /** Env var holding the expected audience. Default `AUTH_AUDIENCE`. */
  audienceEnv?: string;
  /** Audience when that env var is unset. Default `order-service`. */
  defaultAudience?: string;
}

/**
 * Keycloak (docker-compose) defaults; override with AUTH_* environment variables.
 * A service with its own token audience passes `audienceEnv`/`defaultAudience`, so one
 * shared `.env` can't point two services at the same audience.
 */
export function authConfigFromEnv({
  audienceEnv = 'AUTH_AUDIENCE',
  defaultAudience = 'order-service',
}: AuthConfigOptions = {}): AuthConfig {
  const issuer = env('AUTH_ISSUER', 'http://localhost:8081/realms/orderflow').replace(/\/$/, '');
  return {
    issuer,
    audience: env(audienceEnv, defaultAudience),
    jwksUri: env('AUTH_JWKS_URI', `${issuer}/protocol/openid-connect/certs`),
    tokenUrl: env('AUTH_TOKEN_URL', `${issuer}/protocol/openid-connect/token`),
  };
}
