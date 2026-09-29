import {
  AuthError,
  ClientCredentialsTokenProvider,
  Scopes,
  authConfigFromEnv,
  createJwtVerifier,
  remoteJwks,
} from '@orderflow/auth';

export interface Operator {
  clientId: string;
  sub: string;
  getToken: () => Promise<string>;
}

/**
 * Replaying a DLQ re-injects messages into production topics, so the tool must be
 * run as an operator: it obtains a client-credentials token for the "dlq-replay"
 * client and checks (against the IdP's JWKS) that the token carries the `admin` scope.
 */
export async function authorizeOperator(): Promise<Operator> {
  const config = authConfigFromEnv();
  const tokens = new ClientCredentialsTokenProvider({
    tokenUrl: config.tokenUrl,
    clientId: process.env.DLQ_REPLAY_CLIENT_ID ?? 'dlq-replay',
    // Demo secret from infra/keycloak/realm-export.json - not a real credential.
    clientSecret: process.env.DLQ_REPLAY_CLIENT_SECRET ?? 'dlq-replay-demo-secret',
  });
  const verifier = createJwtVerifier({
    issuer: config.issuer,
    audience: config.audience,
    jwks: remoteJwks(config.jwksUri),
  });

  const token = await tokens.getToken();
  let auth;
  try {
    auth = await verifier.verify(token);
  } catch (err) {
    const reason = err instanceof AuthError ? err.message : String(err);
    throw new Error(`Could not verify the dlq-replay token: ${reason}`, { cause: err });
  }
  if (!auth.scopes.has(Scopes.Admin)) {
    throw new Error(
      `Client "${auth.clientId}" does not have the "${Scopes.Admin}" scope required to replay DLQs`,
    );
  }
  return { clientId: auth.clientId, sub: auth.sub, getToken: () => tokens.getToken() };
}

/** Best-effort lookup of an order's status (admin can read any order); undefined if unavailable. */
export async function orderStatus(
  baseUrl: string,
  operator: Operator,
  orderId: string,
): Promise<string | undefined> {
  try {
    const res = await fetch(`${baseUrl}/orders/${encodeURIComponent(orderId)}`, {
      headers: { authorization: `Bearer ${await operator.getToken()}` },
      signal: AbortSignal.timeout(1500),
    });
    if (res.status === 404) return 'unknown';
    if (!res.ok) return undefined;
    return ((await res.json()) as { status?: string }).status;
  } catch {
    return undefined;
  }
}
