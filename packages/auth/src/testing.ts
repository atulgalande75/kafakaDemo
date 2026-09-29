import { SignJWT, createLocalJWKSet, exportJWK, generateKeyPair, type JWTPayload } from 'jose';
import { createJwtVerifier, type TokenVerifier } from './verifier.js';

export const TEST_ISSUER = 'http://issuer.test/realms/orderflow';
export const TEST_AUDIENCE = 'order-service';

export interface TestIssuer {
  verifier: TokenVerifier;
  /** Signs a token; defaults to a valid token for the test issuer and audience. */
  sign(
    claims?: JWTPayload & { scope?: string },
    options?: { expiresIn?: string | number; issuer?: string; audience?: string },
  ): Promise<string>;
  /** A token signed by a different key that the verifier doesn't trust. */
  signWithUnknownKey(claims?: JWTPayload): Promise<string>;
}

/** An in-memory identity provider for tests: RSA key pair + verifier wired to its public key. */
export async function createTestIssuer(): Promise<TestIssuer> {
  const { privateKey, publicKey } = await generateKeyPair('RS256');
  const rogue = await generateKeyPair('RS256');
  const jwk = { ...(await exportJWK(publicKey)), kid: 'test-key', alg: 'RS256' };
  const verifier = createJwtVerifier({
    issuer: TEST_ISSUER,
    audience: TEST_AUDIENCE,
    jwks: createLocalJWKSet({ keys: [jwk] }),
  });

  const build = (claims: JWTPayload, options: Parameters<TestIssuer['sign']>[1] = {}) =>
    new SignJWT({ azp: 'test-client', ...claims })
      .setProtectedHeader({ alg: 'RS256', kid: 'test-key' })
      .setSubject(claims.sub ?? 'user-1')
      .setIssuer(options.issuer ?? TEST_ISSUER)
      .setAudience(options.audience ?? TEST_AUDIENCE)
      .setIssuedAt()
      .setExpirationTime(options.expiresIn ?? '5m');

  return {
    verifier,
    sign: (claims = {}, options) => build(claims, options).sign(privateKey),
    signWithUnknownKey: (claims = {}) => build(claims).sign(rogue.privateKey),
  };
}
