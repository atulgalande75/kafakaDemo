import type { FastifyReply, FastifyRequest, preHandlerAsyncHookHandler } from 'fastify';
import fp from 'fastify-plugin';
import { AuthError } from './errors.js';
import type { AuthContext, TokenVerifier } from './verifier.js';

declare module 'fastify' {
  interface FastifyInstance {
    tokenVerifier: TokenVerifier;
    authRealm: string;
  }
  interface FastifyRequest {
    /** Set by {@link requireScope} once the bearer token has been verified. */
    auth: AuthContext | null;
  }
}

export interface AuthPluginOptions {
  verifier: TokenVerifier;
  /** Value of `realm` in WWW-Authenticate headers. */
  realm?: string;
}

/**
 * Registers the token verifier and turns {@link AuthError}s thrown anywhere in a
 * route into RFC 6750 responses (401 / 403 with WWW-Authenticate, or 503).
 */
export const authPlugin = fp<AuthPluginOptions>(
  (app, { verifier, realm = 'orderflow' }, done) => {
    app.decorate('tokenVerifier', verifier);
    app.decorate('authRealm', realm);
    app.decorateRequest('auth', null);
    done();
  },
  { name: '@orderflow/auth', fastify: '5.x' },
);

export function bearerToken(header: string | undefined): string | undefined {
  const match = /^Bearer\s+([A-Za-z0-9\-._~+/]+=*)$/i.exec(header?.trim() ?? '');
  return match?.[1];
}

/** Verifies the request's bearer token (once per request) and returns the caller. */
export async function authenticate(req: FastifyRequest): Promise<AuthContext> {
  if (req.auth) return req.auth;
  const header = req.headers.authorization;
  const token = bearerToken(header);
  if (!token) {
    throw header
      ? AuthError.invalidToken('Malformed Authorization header')
      : AuthError.missingToken();
  }
  req.auth = await req.server.tokenVerifier.verify(token);
  return req.auth;
}

/**
 * preHandler that requires a valid token carrying at least one of `scopes`.
 * With no scopes it only requires authentication.
 */
export function requireScope(...scopes: string[]): preHandlerAsyncHookHandler {
  return async function (req, reply) {
    try {
      const auth = await authenticate(req);
      if (scopes.length > 0 && !scopes.some((s) => auth.scopes.has(s))) {
        throw AuthError.insufficientScope(scopes);
      }
    } catch (err) {
      if (err instanceof AuthError) return sendAuthError(reply, err);
      throw err;
    }
  };
}

export function hasScope(auth: AuthContext | null, scope: string): boolean {
  return auth?.scopes.has(scope) ?? false;
}

export function sendAuthError(reply: FastifyReply, err: AuthError): FastifyReply {
  const params = [`realm="${reply.server.authRealm ?? 'orderflow'}"`];
  // RFC 6750 3.1: no error code when the request had no credentials at all.
  if (err.code !== 'invalid_request') {
    params.push(`error="${err.code}"`, `error_description="${err.message.replace(/"/g, "'")}"`);
  }
  if (err.requiredScopes.length > 0) params.push(`scope="${err.requiredScopes.join(' ')}"`);

  if (err.statusCode === 503) {
    void reply.header('retry-after', '5');
  } else if (err.code !== 'access_denied') {
    void reply.header('www-authenticate', `Bearer ${params.join(', ')}`);
  }
  return reply.code(err.statusCode).send({ error: err.code, message: err.message });
}
