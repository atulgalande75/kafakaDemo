import { useState, type FormEvent } from 'react';
import { useBulkRestock } from '../api/hooks';

/** One click to top up every SKU that is running low (behind the bulk-adjust-enabled flag). */
export function BulkRestock({ lowSkus }: { lowSkus: string[] }) {
  const restock = useBulkRestock();
  const [quantity, setQuantity] = useState('50');
  const amount = Number(quantity);
  const valid = Number.isInteger(amount) && amount > 0;

  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (valid && lowSkus.length > 0) restock.mutate({ skus: lowSkus, quantity: amount });
  };

  return (
    <form className="adjust" onSubmit={submit} aria-label="Restock low items">
      <h3>Restock low items</h3>
      <div className="row">
        <label>
          Add to each
          <input
            type="number"
            min="1"
            step="1"
            value={quantity}
            onChange={(e) => setQuantity(e.target.value)}
            aria-invalid={!valid}
          />
        </label>
        <button type="submit" disabled={!valid || lowSkus.length === 0 || restock.isPending}>
          {restock.isPending
            ? 'Restocking…'
            : lowSkus.length === 0
              ? 'Nothing is low'
              : `Restock ${lowSkus.length} low ${lowSkus.length === 1 ? 'item' : 'items'}`}
        </button>
      </div>
      {restock.isSuccess && restock.data.restocked.length > 0 && (
        <p className="ok">Restocked {restock.data.restocked.join(', ')}.</p>
      )}
      {restock.isSuccess && restock.data.failed.length > 0 && (
        <p className="error" role="alert">
          Failed: {restock.data.failed.map((f) => `${f.sku} (${f.message})`).join('; ')}
        </p>
      )}
    </form>
  );
}
