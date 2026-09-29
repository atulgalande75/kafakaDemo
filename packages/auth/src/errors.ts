export type AuthErrorCode =
  | 'invalid_request'
  | 'invalid_token'
  | 'insufficient_scope'
  | 'access_denied'
  | 'temporarily_unavailable';

/**
 * An authentication/authorization failure with the HTTP status it maps to:
 *   401 - no token, or the token is invalid/expired/for someone else (iss, aud)
 *   403 - valid token, but missing scope or not allowed to touch this resource
 *   503 - the identity provider's keys (JWKS) could not be fetched
 */
export class AuthError extends Error {
  override readonly name = 'AuthError';

  constructor(
    readonly statusCode: 401 | 403 | 503,
    readonly code: AuthErrorCode,
    message: string,
    readonly requiredScopes: readonly string[] = [],
  ) {
    super(message);
  }

  static missingToken() {
    return new AuthError(401, 'invalid_request', 'Missing bearer token');
  }

  static invalidToken(message: string) {
    return new AuthError(401, 'invalid_token', message);
  }

  static insufficientScope(scopes: readonly string[]) {
    return new AuthError(
      403,
      'insufficient_scope',
      `Requires scope: ${scopes.join(' or ')}`,
      scopes,
    );
  }

  static accessDenied(message: string) {
    return new AuthError(403, 'access_denied', message);
  }

  static unavailable(message: string) {
    return new AuthError(503, 'temporarily_unavailable', message);
  }
}
