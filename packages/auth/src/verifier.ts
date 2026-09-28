import { createRemoteJWKSet, errors, jwtVerify, type JWTPayload, type JWTVerifyGetKey } from 'jose';
import { AuthError } from './errors.js';

/** Who is calling, derived from a verified access token. Never contains the token itself. */
export interface AuthContext {
  /** Subject: the user id, or the service account id for client-credentials tokens. */
  sub: string;
  /** The OAuth client the token was issued to (`azp`). */
  clientId: string;
  scopes: ReadonlySet<string>;
  claims: JWTPayload;
}

export interface TokenVerifier {
  /** Resolves with the caller's identity or rejects with an {@link AuthError}. */
  verify(token: string): Promise<AuthContext>;
}

export interface JwtVerifierOptions {
  issuer: string;
  audience: string;
  /** Key lookup - usually {@link remoteJwks}; tests pass a local key set. */
  jwks: JWTVerifyGetKey;
  algorithms?: string[];
  clockToleranceSec?: number;
}

/** JWKS fetched from the identity provider, cached, and re-fetched when an unknown `kid` shows up. */
export function remoteJwks(jwksUri: string): JWTVerifyGetKey {
  return createRemoteJWKSet(new URL(jwksUri), { timeoutDuration: 3000, cooldownDuration: 30_000 });
}

export function scopesOf(claims: JWTPayload): Set<string> {
  const scope = claims.scope;
  const scp = claims.scp;
  const list = typeof scope === 'string' ? scope.split(' ') : Array.isArray(scp) ? scp : [];
  return new Set(list.filter((s): s is string => typeof s === 'string' && s.length > 0));
}

/** Verifies signature (via JWKS), `iss`, `aud`, `exp`/`nbf` and the presence of `sub`. */
export function createJwtVerifier(options: JwtVerifierOptions): TokenVerifier {
  const { issuer, audience, jwks, algorithms = ['RS256'], clockToleranceSec = 5 } = options;

  return {
    async verify(token) {
      let claims: JWTPayload;
      try {
        ({ payload: claims } = await jwtVerify(token, jwks, {
          issuer,
          audience,
          algorithms,
          clockTolerance: clockToleranceSec,
          requiredClaims: ['exp', 'sub'],
        }));
      } catch (err) {
        throw toAuthError(err);
      }

      const clientId = claims.azp ?? claims.client_id;
      return {
        sub: claims.sub!,
        clientId: typeof clientId === 'string' ? clientId : 'unknown',
        scopes: scopesOf(claims),
        claims,
      };
    },
  };
}

function toAuthError(err: unknown): AuthError {
  if (err instanceof errors.JWTExpired) return AuthError.invalidToken('Token expired');
  if (err instanceof errors.JWTClaimValidationFailed) {
    return AuthError.invalidToken(`Invalid "${err.claim}" claim: ${err.reason}`);
  }
  if (err instanceof errors.JWKSTimeout || err instanceof errors.JWKSInvalid) {
    return AuthError.unavailable('Identity provider keys are unavailable');
  }
  if (err instanceof errors.JOSEError) return AuthError.invalidToken('Invalid token');
  // Anything else is a network failure fetching the JWKS (e.g. Keycloak is down).
  return AuthError.unavailable('Identity provider is unreachable');
}
