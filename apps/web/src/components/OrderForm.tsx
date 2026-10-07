import { useState, type FormEvent } from 'react';
import { useAuth } from '../auth/AuthProvider';
import { useCreateOrder } from '../api/hooks';
import { CARD_LIMIT, CATALOG, COUNTRIES, TIERS } from '../catalog';
import { formatMoney, shortId } from '../format';

interface Line {
  sku: string;
  quantity: string;
}

export function OrderForm() {
  const { username, scopes } = useAuth();
  const create = useCreateOrder();
  const [lines, setLines] = useState<Line[]>([{ sku: 'SKU-MOUSE', quantity: '1' }]);
  const [tier, setTier] = useState<(typeof TIERS)[number]>('standard');
  const [country, setCountry] = useState<(typeof COUNTRIES)[number]>('US');

  const priceOf = (sku: string) => CATALOG.find((c) => c.sku === sku)?.price ?? 0;
  const parsed = lines.map((l) => ({
    sku: l.sku,
    quantity: Number(l.quantity),
    unitPrice: priceOf(l.sku),
  }));
  const valid =
    parsed.length > 0 && parsed.every((l) => Number.isInteger(l.quantity) && l.quantity > 0);
  const total = parsed.reduce((sum, l) => sum + l.quantity * l.unitPrice, 0);

  const update = (index: number, patch: Partial<Line>) =>
    setLines((current) => current.map((l, i) => (i === index ? { ...l, ...patch } : l)));

  const submit = (event: FormEvent) => {
    event.preventDefault();
    if (!valid || !username) return;
    create.mutate({ customerId: username, customerTier: tier, country, items: parsed });
  };

  if (!scopes.has('orders:write')) {
    return (
      <section className="card">
        <h2>Place an order</h2>
        <p className="muted">Your account is not allowed to place orders.</p>
      </section>
    );
  }

  return (
    <section className="card" aria-labelledby="order-title">
      <h2 id="order-title">Place an order</h2>
      <form onSubmit={submit} aria-label="Place an order">
        {lines.map((line, index) => (
          <div className="row" key={index}>
            <label className="grow">
              Product
              <select value={line.sku} onChange={(e) => update(index, { sku: e.target.value })}>
                {CATALOG.map((c) => (
                  <option key={c.sku} value={c.sku}>
                    {c.name} ({formatMoney(c.price)})
                  </option>
                ))}
              </select>
            </label>
            <label>
              Qty
              <input
                type="number"
                min="1"
                step="1"
                value={line.quantity}
                onChange={(e) => update(index, { quantity: e.target.value })}
              />
            </label>
            {lines.length > 1 && (
              <button
                type="button"
                className="ghost"
                aria-label={`Remove line ${index + 1}`}
                onClick={() => setLines((current) => current.filter((_, i) => i !== index))}
              >
                Remove
              </button>
            )}
          </div>
        ))}
        <div className="row">
          <button
            type="button"
            className="ghost"
            disabled={lines.length >= 5}
            onClick={() =>
              setLines((current) => [...current, { sku: 'SKU-KEYBOARD', quantity: '1' }])
            }
          >
            Add item
          </button>
          <label>
            Tier
            <select value={tier} onChange={(e) => setTier(e.target.value as typeof tier)}>
              {TIERS.map((t) => (
                <option key={t}>{t}</option>
              ))}
            </select>
          </label>
          <label>
            Country
            <select value={country} onChange={(e) => setCountry(e.target.value as typeof country)}>
              {COUNTRIES.map((c) => (
                <option key={c}>{c}</option>
              ))}
            </select>
          </label>
        </div>
        <div className="row between">
          <strong>Total {formatMoney(total)}</strong>
          <button type="submit" disabled={!valid || create.isPending}>
            {create.isPending ? 'Placing…' : 'Place order'}
          </button>
        </div>
        {total > CARD_LIMIT && (
          <p className="hint">
            Orders above {formatMoney(CARD_LIMIT)} are declined by the payment service, and the
            reserved stock is released again. Try it.
          </p>
        )}
        {create.isError && (
          <p className="error" role="alert">
            {create.error.message}
          </p>
        )}
        {create.isSuccess && <p className="ok">Order {shortId(create.data.id)} placed.</p>}
      </form>
    </section>
  );
}
