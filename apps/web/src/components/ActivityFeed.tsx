import { useState } from 'react';
import { formatTime } from '../format';
import { useLive } from '../live/LiveProvider';

type Filter = 'all' | 'order' | 'stock-alert';
const FILTERS: Array<[Filter, string]> = [
  ['all', 'All'],
  ['order', 'Orders'],
  ['stock-alert', 'Alerts'],
];

export function ActivityFeed() {
  const { feed, flags } = useLive();
  const [filter, setFilter] = useState<Filter>('all');
  const entries = (filter === 'all' ? feed : feed.filter((e) => e.kind === filter)).slice(
    0,
    flags.activityFeedSize,
  );

  return (
    <section className="card feed" aria-labelledby="feed-title">
      <div className="row between">
        <h2 id="feed-title">Live activity</h2>
        <div className="tabs" role="group" aria-label="Filter activity">
          {FILTERS.map(([value, label]) => (
            <button
              key={value}
              type="button"
              className={filter === value ? 'tab active' : 'tab'}
              aria-pressed={filter === value}
              onClick={() => setFilter(value)}
            >
              {label}
            </button>
          ))}
        </div>
      </div>
      {entries.length === 0 ? (
        <p className="muted">Nothing yet. Events show up here the moment they happen.</p>
      ) : (
        <ul className="events">
          {entries.map((entry) => (
            <li key={entry.seq} className={entry.kind === 'stock-alert' ? 'alert' : undefined}>
              <span className="muted mono">{formatTime(entry.occurredAt)}</span>
              <span className={`chip ${chipClass(entry.type)}`}>{entry.type}</span>
              <span>{entry.summary}</span>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

function chipClass(type: string): string {
  if (type === 'stock.low') return 'warn';
  if (type.endsWith('failed') || type.endsWith('rejected')) return 'bad';
  if (type.endsWith('completed') || type.endsWith('reserved')) return 'good';
  return '';
}
