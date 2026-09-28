export interface ClientCredentialsOptions {
  tokenUrl: string;
  clientId: string;
  clientSecret: string;
  /** Space-separated scopes to request; omit to get the client's default scopes. */
  scope?: string;
  /** Refresh this many seconds before the token expires. */
  refreshSkewSec?: number;
  fetchImpl?: typeof fetch;
  now?: () => number;
}

/**
 * OAuth2 client-credentials flow (RFC 6749 4.4) for machine clients such as the
 * load generator and the DLQ replay tool. Tokens are cached until shortly before
 * they expire and concurrent callers share one request.
 */
export class ClientCredentialsTokenProvider {
  private cached?: { token: string; expiresAt: number };
  private inflight?: Promise<string>;

  constructor(private readonly options: ClientCredentialsOptions) {}

  getToken(): Promise<string> {
    const now = (this.options.now ?? Date.now)();
    if (this.cached && now < this.cached.expiresAt) return Promise.resolve(this.cached.token);
    this.inflight ??= this.fetchToken().finally(() => (this.inflight = undefined));
    return this.inflight;
  }

  private async fetchToken(): Promise<string> {
    const { tokenUrl, clientId, clientSecret, scope, refreshSkewSec = 30 } = this.options;
    const body = new URLSearchParams({ grant_type: 'client_credentials' });
    if (scope) body.set('scope', scope);

    let res: Response;
    try {
      res = await (this.options.fetchImpl ?? fetch)(tokenUrl, {
        method: 'POST',
        headers: {
          'content-type': 'application/x-www-form-urlencoded',
          authorization: `Basic ${Buffer.from(`${encodeURIComponent(clientId)}:${encodeURIComponent(clientSecret)}`).toString('base64')}`,
        },
        body,
      });
    } catch (err) {
      throw new Error(`Cannot reach the identity provider at ${tokenUrl} - is Keycloak running?`, {
        cause: err,
      });
    }

    const json = (await res.json().catch(() => ({}))) as {
      access_token?: string;
      expires_in?: number;
      error?: string;
      error_description?: string;
    };
    if (!res.ok || !json.access_token) {
      throw new Error(
        `Token request for client "${clientId}" failed: ${res.status} ${json.error ?? ''} ${json.error_description ?? ''}`.trim(),
      );
    }

    const lifetimeMs = Math.max(0, (json.expires_in ?? 60) - refreshSkewSec) * 1000;
    this.cached = {
      token: json.access_token,
      expiresAt: (this.options.now ?? Date.now)() + lifetimeMs,
    };
    return json.access_token;
  }
}
