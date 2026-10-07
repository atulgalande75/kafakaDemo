import type { ConnectionStatus } from '../live/reducer';

const LABEL: Record<ConnectionStatus, string> = {
  connecting: 'Connecting…',
  live: 'Live',
  polling: 'Polling',
  reconnecting: 'Reconnecting…',
  error: 'Disconnected',
};

export function ConnectionBadge({ status, error }: { status: ConnectionStatus; error?: string }) {
  return (
    <span
      className={`badge conn conn-${status}`}
      role="status"
      title={
        status === 'polling' ? 'Live updates are switched off: refreshing every few seconds' : error
      }
    >
      <span className="dot" aria-hidden="true" />
      {LABEL[status]}
    </span>
  );
}
