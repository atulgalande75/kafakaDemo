import { useAuth } from '../auth/AuthProvider';
import { useLive } from '../live/LiveProvider';
import { ConnectionBadge } from './ConnectionBadge';

export function Header() {
  const { username, logout } = useAuth();
  const { status, error } = useLive();
  return (
    <header className="topbar">
      <h1>
        Orderflow <span className="muted">live inventory</span>
      </h1>
      <div className="topbar-right">
        <ConnectionBadge status={status} error={error} />
        <span className="muted">{username}</span>
        <button type="button" className="ghost" onClick={() => void logout()}>
          Sign out
        </button>
      </div>
    </header>
  );
}
