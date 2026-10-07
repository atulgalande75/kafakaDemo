/** Splits an OAuth `scope` string ("a b c") into a set. */
export function parseScopes(scope: string | undefined | null): ReadonlySet<string> {
  return new Set((scope ?? '').split(/\s+/).filter(Boolean));
}
