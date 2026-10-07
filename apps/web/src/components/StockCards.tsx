import type { StockView } from '@orderflow/stream-types';
import { Available, Level, StatusBadge, lastChangeText } from './stock-parts';

/** The "new inventory dashboard": one card per SKU instead of a table row. */
export function StockCards({
  items,
  pulses,
}: {
  items: StockView[];
  pulses: Record<string, number>;
}) {
  return (
    <ul className="cards" aria-label="Stock levels">
      {items.map((item) => (
        <li key={item.sku} className="stock-card" data-sku={item.sku}>
          <div className="row between">
            <strong>{item.name}</strong>
            <StatusBadge item={item} />
          </div>
          <div className="mono muted">{item.sku}</div>
          <div className="big">
            <Available item={item} pulse={pulses[item.sku] ?? 0} />
          </div>
          <Level item={item} />
          <div className="muted">{lastChangeText(item)}</div>
        </li>
      ))}
    </ul>
  );
}
