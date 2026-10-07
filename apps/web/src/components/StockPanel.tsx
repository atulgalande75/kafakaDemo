import { useAuth } from '../auth/AuthProvider';
import { useLive } from '../live/LiveProvider';
import { AdjustStock } from './AdjustStock';
import { BulkRestock } from './BulkRestock';
import { StockCards } from './StockCards';
import { Available, Level, StatusBadge, lastChangeText } from './stock-parts';

export function StockPanel() {
  const { scopes } = useAuth();
  const { stock, pulses, status, flags } = useLive();
  const items = stock ? Object.values(stock).sort((a, b) => a.sku.localeCompare(b.sku)) : null;
  const canWrite = scopes.has('inventory:write');

  return (
    <section className="card" aria-labelledby="stock-title">
      <h2 id="stock-title">Stock</h2>
      {items === null ? (
        <p className="muted">
          {status === 'live' || status === 'polling'
            ? 'Your account is not allowed to view stock levels.'
            : 'Waiting for the first snapshot…'}
        </p>
      ) : flags.newInventoryDashboard ? (
        <StockCards items={items} pulses={pulses} />
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>SKU</th>
                <th>Product</th>
                <th className="num">Available</th>
                <th>Level</th>
                <th>Status</th>
                <th>Last change</th>
              </tr>
            </thead>
            <tbody>
              {items.map((item) => (
                <tr key={item.sku} data-sku={item.sku}>
                  <td className="mono">{item.sku}</td>
                  <td>{item.name}</td>
                  <td className="num">
                    <Available item={item} pulse={pulses[item.sku] ?? 0} />
                  </td>
                  <td>
                    <Level item={item} />
                  </td>
                  <td>
                    <StatusBadge item={item} />
                  </td>
                  <td className="muted">{lastChangeText(item)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {items !== null && canWrite && <AdjustStock skus={items.map((i) => i.sku)} />}
      {items !== null && canWrite && flags.bulkAdjust && (
        <BulkRestock lowSkus={items.filter((i) => i.low).map((i) => i.sku)} />
      )}
    </section>
  );
}
