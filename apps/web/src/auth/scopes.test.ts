import { describe, expect, it } from 'vitest';
import { parseScopes } from './scopes';

describe('parseScopes', () => {
  it('splits on whitespace and ignores blanks', () => {
    expect([...parseScopes(' openid  orders:read\tinventory:write ')]).toEqual([
      'openid',
      'orders:read',
      'inventory:write',
    ]);
  });

  it('is empty for missing scopes', () => {
    expect(parseScopes(undefined).size).toBe(0);
    expect(parseScopes('').size).toBe(0);
  });
});
