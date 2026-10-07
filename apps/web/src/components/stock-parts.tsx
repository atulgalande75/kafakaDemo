import type { StockView } from '@orderflow/stream-types';
import { signed } from '../format';

export function Level({ item }: { item: StockView }) {
  const scale = Math.max(item.lowStockThreshold * 6, item.available, 1);
  const state = item.available === 0 ? 'out' : item.low ? 'low' : 'ok';
  return (
    <div className="level" aria-hidden="true">
      <div
        className={`level-fill level-${state}`}
        style={{ width: `${(item.available / scale) * 100}%` }}
      />
    </div>
  );
}

export function StatusBadge({ item }: { item: StockView }) {
  if (item.available === 0) return <span className="badge bad">Out of stock</span>;
  if (item.low) return <span className="badge warn">Low</span>;
  return <span className="badge good">In stock</span>;
}

/** "−3 reserved", or nothing for a plain resync. */
export const lastChangeText = (item: StockView) =>
  item.lastChange.reason === 'snapshot'
    ? '–'
    : `${signed(item.lastChange.delta)} ${item.lastChange.reason}`;

/** The available count, which flashes whenever a live update arrives for the SKU. */
export function Available({ item, pulse }: { item: StockView; pulse: number }) {
  return (
    <span key={pulse} className={pulse > 0 ? 'flash' : undefined}>
      {item.available.toLocaleString()}
    </span>
  );
}
