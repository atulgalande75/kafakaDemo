import { useState, type FormEvent } from 'react';
import { useAdjustStock } from '../api/hooks';
import type { AdjustReason } from '../api/types';

const HINT: Record<AdjustReason, string> = {
  restock: 'Adds stock (a positive number).',
  shrinkage: 'Removes stock: damaged or lost (a negative number).',
  correction: 'Fixes a counting mistake (positive or negative).',
};

/** Where `reason` and the sign of the quantity must agree (the API rejects a mismatch). */
export function toDelta(reason: AdjustReason, quantity: number): number {
  if (reason === 'restock') return Math.abs(quantity);
  if (reason === 'shrinkage') return -Math.abs(quantity);
  return quantity;
}

export function AdjustStock({ skus }: { skus: string[] }) {
  const adjust = useAdjustStock();
  const [sku, setSku] = useState(skus[0] ?? '');
  const [reason, setReason] = useState<AdjustReason>('restock');
  const [quantity, setQuantity] = useState('10');
  const [note, setNote] = useState('');

  const amount = Number(quantity);
  const valid = Number.isInteger(amount) && amount !== 0 && skus.includes(sku);

  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (!valid) return;
    adjust.mutate({
      sku,
      reason,
      delta: toDelta(reason, amount),
      ...(note.trim() && { note: note.trim() }),
    });
  };

  return (
    <form className="adjust" onSubmit={submit} aria-label="Adjust stock">
      <h3>Adjust stock</h3>
      <div className="row">
        <label>
          SKU
          <select value={sku} onChange={(e) => setSku(e.target.value)}>
            {skus.map((s) => (
              <option key={s}>{s}</option>
            ))}
          </select>
        </label>
        <label>
          Reason
          <select value={reason} onChange={(e) => setReason(e.target.value as AdjustReason)}>
            <option value="restock">Restock</option>
            <option value="shrinkage">Shrinkage</option>
            <option value="correction">Correction</option>
          </select>
        </label>
        <label>
          Quantity
          <input
            type="number"
            step="1"
            value={quantity}
            onChange={(e) => setQuantity(e.target.value)}
            aria-invalid={!valid}
          />
        </label>
        <label className="grow">
          Note
          <input
            value={note}
            maxLength={500}
            onChange={(e) => setNote(e.target.value)}
            placeholder="optional"
          />
        </label>
        <button type="submit" disabled={!valid || adjust.isPending}>
          {adjust.isPending ? 'Applying…' : 'Apply'}
        </button>
      </div>
      <p className="hint">{HINT[reason]}</p>
      {adjust.isError && (
        <p className="error" role="alert">
          {adjust.error.message}
        </p>
      )}
      {adjust.isSuccess && (
        <p className="ok">Applied. The table updates as the change comes back over the stream.</p>
      )}
    </form>
  );
}
