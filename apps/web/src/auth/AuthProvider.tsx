import { UserManager, WebStorageStateStore, type User } from 'oidc-client-ts';
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react';
import { config } from '../config';
import { parseScopes } from './scopes';

/**
 * What the rest of the app knows about authentication. Components and the API client only
 * use this interface, so the identity provider behind it (Keycloak today) can be swapped.
 */
export interface AuthState {
  status: 'loading' | 'authenticated' | 'unauthenticated' | 'error';
  error?: string;
  /** Display name of the signed-in user. */
  username?: string;
  /** Subject of the access token (matches the `actor` of the user's events). */
  sub?: string;
  scopes: ReadonlySet<string>;
  /** A valid access token, renewed first if it is about to expire. */
  getAccessToken: () => Promise<string>;
  login: () => Promise<void>;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthState | undefined>(undefined);

export function useAuth(): AuthState {
  const value = useContext(AuthContext);
  if (!value) throw new Error('useAuth must be used inside <AuthProvider>');
  return value;
}

const origin = () => window.location.origin;

function createManager(): UserManager {
  return new UserManager({
    authority: config.oidc.authority,
    client_id: config.oidc.clientId,
    redirect_uri: `${origin()}/`,
    post_logout_redirect_uri: `${origin()}/`,
    response_type: 'code', // authorization code flow; PKCE is on by default
    scope: config.oidc.scope,
    // Tokens live only for this tab, and are gone when it closes.
    userStore: new WebStorageStateStore({ store: window.sessionStorage }),
    automaticSilentRenew: true, // renews with the refresh token before the access token expires
  });
}

const isCallback = () => {
  const params = new URLSearchParams(window.location.search);
  return (params.has('code') || params.has('error')) && params.has('state');
};

/**
 * One manager and one start-up promise per page: React StrictMode runs effects twice in
 * development, and an authorization code can only be exchanged once.
 */
let manager: UserManager | undefined;
let starting: Promise<User | null> | undefined;

function start(): { manager: UserManager; user: Promise<User | null> } {
  manager ??= createManager();
  starting ??= (async () => {
    if (isCallback()) {
      const user = await manager.signinCallback();
      window.history.replaceState({}, document.title, '/'); // drop ?code=&state= from the URL
      return user ?? null;
    }
    const user = await manager.getUser();
    return user && !user.expired ? user : null;
  })();
  return { manager, user: starting };
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [status, setStatus] = useState<AuthState['status']>('loading');
  const [error, setError] = useState<string>();

  useEffect(() => {
    const { manager: m, user: initial } = start();
    let active = true;
    initial
      .then((u) => {
        if (!active) return;
        setUser(u);
        setStatus(u ? 'authenticated' : 'unauthenticated');
      })
      .catch((err: unknown) => {
        if (!active) return;
        setError(err instanceof Error ? err.message : String(err));
        setStatus('error');
      });

    const onLoaded = (u: User) => setUser(u);
    const onGone = () => {
      setUser(null);
      setStatus('unauthenticated');
    };
    m.events.addUserLoaded(onLoaded);
    m.events.addUserUnloaded(onGone);
    m.events.addAccessTokenExpired(onGone);
    return () => {
      active = false;
      m.events.removeUserLoaded(onLoaded);
      m.events.removeUserUnloaded(onGone);
      m.events.removeAccessTokenExpired(onGone);
    };
  }, []);

  const getAccessToken = useCallback(async () => {
    const m = start().manager;
    let current = await m.getUser();
    if (!current || current.expired) current = await m.signinSilent();
    if (!current) throw new Error('Not signed in');
    return current.access_token;
  }, []);

  const value = useMemo<AuthState>(
    () => ({
      status,
      error,
      username: user?.profile.preferred_username ?? user?.profile.sub,
      sub: user?.profile.sub,
      scopes: parseScopes(user?.scope),
      getAccessToken,
      login: () => start().manager.signinRedirect(),
      logout: () => start().manager.signoutRedirect(),
    }),
    [status, error, user, getAccessToken],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
