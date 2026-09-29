import Fastify from 'fastify';
import { beforeAll, describe, expect, it, vi } from 'vitest';
import { ClientCredentialsTokenProvider } from './client-credentials.js';
import { AuthError } from './errors.js';
import { authPlugin, bearerToken, requireScope } from './plugin.js';
import { createTestIssuer, type TestIssuer } from './testing.js';
import { createJwtVerifier, scopesOf } from './verifier.js';

let issuer: TestIssuer;
beforeAll(async () => {
  issuer = await createTestIssuer();
});

function app() {
  const server = Fastify();
  void server.register(authPlugin, { verifier: issuer.verifier });
  server.get('/read', { preHandler: requireScope('orders:read', 'admin') }, (req) => ({
    sub: req.auth?.sub,
    clientId: req.auth?.clientId,
  }));
  server.get('/any', { preHandler: requireScope() }, () => ({ ok: true }));
  return server;
}

const get = (url: string, token?: string, raw?: string) =>
  app().inject({
    method: 'GET',
    url,
    headers: raw ? { authorization: raw } : token ? { authorization: `Bearer ${token}` } : {},
  });

describe('requireScope', () => {
  it('200 with a valid token that has the scope', async () => {
    const res = await get('/read', await issuer.sign({ sub: 'u-1', scope: 'orders:read' }));
    expect(res.statusCode).toBe(200);
    expect(res.json()).toEqual({ sub: 'u-1', clientId: 'test-client' });
  });

  it('accepts any of the listed scopes', async () => {
    const res = await get('/read', await issuer.sign({ scope: 'admin' }));
    expect(res.statusCode).toBe(200);
  });

  it('401 without a token, WWW-Authenticate without an error code', async () => {
    const res = await get('/read');
    expect(res.statusCode).toBe(401);
    expect(res.headers['www-authenticate']).toBe('Bearer realm="orderflow"');
    expect(res.json()).toMatchObject({ error: 'invalid_request' });
  });

  it('401 for a malformed Authorization header', async () => {
    const res = await get('/read', undefined, 'Basic dXNlcjpwYXNz');
    expect(res.statusCode).toBe(401);
    expect(res.json()).toMatchObject({ error: 'invalid_token' });
  });

  it('401 for garbage that is not a JWT', async () => {
    const res = await get('/read', 'not-a-jwt');
    expect(res.statusCode).toBe(401);
    expect(res.headers['www-authenticate']).toContain('error="invalid_token"');
  });

  it('401 for an expired token', async () => {
    const past = Math.floor(Date.now() / 1000) - 60;
    const res = await get(
      '/read',
      await issuer.sign({ scope: 'orders:read' }, { expiresIn: past }),
    );
    expect(res.statusCode).toBe(401);
    expect(res.json()).toMatchObject({ error: 'invalid_token', message: 'Token expired' });
  });

  it('401 for the wrong issuer', async () => {
    const token = await issuer.sign({ scope: 'orders:read' }, { issuer: 'https://evil.example' });
    const res = await get('/read', token);
    expect(res.statusCode).toBe(401);
    expect(res.json<{ message: string }>().message).toContain('"iss"');
  });

  it('401 for the wrong audience', async () => {
    const token = await issuer.sign({ scope: 'orders:read' }, { audience: 'another-api' });
    const res = await get('/read', token);
    expect(res.statusCode).toBe(401);
    expect(res.json<{ message: string }>().message).toContain('"aud"');
  });

  it('401 for a token signed by an untrusted key', async () => {
    const res = await get('/read', await issuer.signWithUnknownKey({ scope: 'orders:read' }));
    expect(res.statusCode).toBe(401);
  });

  it('403 with insufficient_scope when the scope is missing', async () => {
    const res = await get('/read', await issuer.sign({ scope: 'orders:write' }));
    expect(res.statusCode).toBe(403);
    expect(res.headers['www-authenticate']).toBe(
      'Bearer realm="orderflow", error="insufficient_scope", error_description="Requires scope: orders:read or admin", scope="orders:read admin"',
    );
  });

  it('only authenticates when no scope is required', async () => {
    expect((await get('/any', await issuer.sign({}))).statusCode).toBe(200);
    expect((await get('/any')).statusCode).toBe(401);
  });

  it('503 when the identity provider keys cannot be fetched', async () => {
    const server = Fastify();
    void server.register(authPlugin, {
      verifier: createJwtVerifier({
        issuer: 'x',
        audience: 'y',
        jwks: () => Promise.reject(new TypeError('fetch failed')),
      }),
    });
    server.get('/read', { preHandler: requireScope('orders:read') }, () => 'ok');
    const res = await server.inject({
      method: 'GET',
      url: '/read',
      headers: { authorization: `Bearer ${await issuer.sign({})}` },
    });
    expect(res.statusCode).toBe(503);
    expect(res.headers['retry-after']).toBe('5');
  });
});

describe('helpers', () => {
  it('extracts bearer tokens', () => {
    expect(bearerToken('Bearer abc.def.ghi')).toBe('abc.def.ghi');
    expect(bearerToken('bearer abc')).toBe('abc');
    expect(bearerToken('Basic abc')).toBeUndefined();
    expect(bearerToken(undefined)).toBeUndefined();
  });

  it('reads scopes from "scope" or "scp"', () => {
    expect([...scopesOf({ scope: 'a b' })]).toEqual(['a', 'b']);
    expect([...scopesOf({ scp: ['c'] })]).toEqual(['c']);
    expect(scopesOf({}).size).toBe(0);
  });

  it('AuthError factories map to the right status codes', () => {
    expect(AuthError.missingToken().statusCode).toBe(401);
    expect(AuthError.insufficientScope(['x']).statusCode).toBe(403);
    expect(AuthError.accessDenied('no').statusCode).toBe(403);
    expect(AuthError.unavailable('down').statusCode).toBe(503);
  });
});

describe('ClientCredentialsTokenProvider', () => {
  const tokenResponse = (token: string, expiresIn = 300) =>
    new Response(JSON.stringify({ access_token: token, expires_in: expiresIn }), { status: 200 });

  it('requests a token with client credentials and caches it until near expiry', async () => {
    let now = 0;
    const fetchImpl = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(tokenResponse('t1', 60))
      .mockResolvedValueOnce(tokenResponse('t2', 60));
    const provider = new ClientCredentialsTokenProvider({
      tokenUrl: 'http://idp/token',
      clientId: 'load-generator',
      clientSecret: 's3cret',
      fetchImpl,
      now: () => now,
    });

    expect(await provider.getToken()).toBe('t1');
    now = 20_000;
    expect(await provider.getToken()).toBe('t1'); // cached (60s - 30s skew)
    now = 31_000;
    expect(await provider.getToken()).toBe('t2');

    const [url, init] = fetchImpl.mock.calls[0]!;
    expect(url).toBe('http://idp/token');
    expect((init?.body as URLSearchParams).toString()).toBe('grant_type=client_credentials');
    expect((init?.headers as Record<string, string>).authorization).toBe(
      `Basic ${Buffer.from('load-generator:s3cret').toString('base64')}`,
    );
  });

  it('shares one request between concurrent callers', async () => {
    const fetchImpl = vi.fn<typeof fetch>().mockResolvedValue(tokenResponse('t'));
    const provider = new ClientCredentialsTokenProvider({
      tokenUrl: 'http://idp/token',
      clientId: 'c',
      clientSecret: 's',
      fetchImpl,
    });
    await Promise.all([provider.getToken(), provider.getToken(), provider.getToken()]);
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it('reports token endpoint errors', async () => {
    const provider = new ClientCredentialsTokenProvider({
      tokenUrl: 'http://idp/token',
      clientId: 'c',
      clientSecret: 'wrong',
      fetchImpl: () =>
        Promise.resolve(
          new Response(
            JSON.stringify({ error: 'unauthorized_client', error_description: 'Invalid client' }),
            { status: 401 },
          ),
        ),
    });
    await expect(provider.getToken()).rejects.toThrow(/401 unauthorized_client Invalid client/);
  });
});
