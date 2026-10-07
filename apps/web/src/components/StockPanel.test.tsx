import { render, screen, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { DEFAULT_UI_FLAGS } from '../flags/defaults';
import { initialLiveState, type LiveState } from '../live/reducer';
import { stockView } from '../test/fixtures';
import { StockPanel } from './StockPanel';

const hoisted = vi.hoisted(() => ({
  scopes: new Set<string>(),
  live: undefined as object | undefined,
}));
vi.mock('../auth/AuthProvider', () => ({ useAuth: () => ({ scopes: hoisted.scopes }) }));
vi.mock('../live/LiveProvider', () => ({ useLive: () => hoisted.live }));
vi.mock('../api/hooks', () => ({
  useAdjustStock: () => ({ mutate: vi.fn(), isPending: false, isError: false, isSuccess: false }),
  useBulkRestock: () => ({ mutate: vi.fn(), isPending: false, isSuccess: false }),
}));

const live = (over: Partial<LiveState>): LiveState => ({
  ...initialLiveState,
  status: 'live',
  ...over,
});
const row = (sku: string) => screen.getByText(sku).closest('tr')!;

beforeEach(() => {
  hoisted.scopes = new Set(['inventory:read']);
});

describe('StockPanel', () => {
  it('shows a waiting message before the first snapshot', () => {
    hoisted.live = live({ status: 'connecting' });
    render(<StockPanel />);
    expect(screen.getByText(/waiting for the first snapshot/i)).toBeInTheDocument();
  });

  it('explains when the user may not see stock', () => {
    hoisted.live = live({ stock: null });
    render(<StockPanel />);
    expect(screen.getByText(/not allowed to view stock/i)).toBeInTheDocument();
  });

  it('lists SKUs sorted, with a status for each stock level', () => {
    hoisted.live = live({
      stock: {
        'SKU-B': stockView({ sku: 'SKU-B', name: 'Beta', available: 3, low: true }),
        'SKU-A': stockView({ sku: 'SKU-A', name: 'Alpha', available: 50 }),
        'SKU-C': stockView({ sku: 'SKU-C', name: 'Gamma', available: 0, low: true }),
      },
    });
    render(<StockPanel />);

    const skus = screen
      .getAllByRole('row')
      .slice(1)
      .map((r) => within(r).getAllByRole('cell')[0]?.textContent);
    expect(skus).toEqual(['SKU-A', 'SKU-B', 'SKU-C']);
    expect(within(row('SKU-A')).getByText('In stock')).toBeInTheDocument();
    expect(within(row('SKU-B')).getByText('Low')).toBeInTheDocument();
    expect(within(row('SKU-C')).getByText('Out of stock')).toBeInTheDocument();
  });

  it('describes the last change, and shows nothing for a plain resync', () => {
    hoisted.live = live({
      stock: {
        'SKU-A': stockView({ sku: 'SKU-A', lastChange: { delta: -3, reason: 'reserved' } }),
        'SKU-B': stockView({ sku: 'SKU-B', lastChange: { delta: 0, reason: 'snapshot' } }),
      },
    });
    render(<StockPanel />);
    expect(within(row('SKU-A')).getByText('−3 reserved')).toBeInTheDocument();
    expect(within(row('SKU-B')).queryByText(/snapshot/)).not.toBeInTheDocument();
  });

  it('highlights a row only after a live update', () => {
    hoisted.live = live({
      stock: {
        'SKU-A': stockView({ sku: 'SKU-A', available: 7 }),
        'SKU-B': stockView({ sku: 'SKU-B', available: 9 }),
      },
      pulses: { 'SKU-A': 2 },
    });
    render(<StockPanel />);
    expect(within(row('SKU-A')).getByText('7')).toHaveClass('flash');
    expect(within(row('SKU-B')).getByText('9')).not.toHaveClass('flash');
  });

  it('offers stock adjustment only to users with inventory:write', () => {
    hoisted.live = live({ stock: { 'SKU-A': stockView() } });
    const { unmount } = render(<StockPanel />);
    expect(screen.queryByRole('form', { name: /adjust stock/i })).not.toBeInTheDocument();
    unmount();

    hoisted.scopes = new Set(['inventory:read', 'inventory:write']);
    render(<StockPanel />);
    expect(screen.getByRole('form', { name: /adjust stock/i })).toBeInTheDocument();
  });

  describe('feature flags', () => {
    const stock = () => ({
      'SKU-A': stockView({ sku: 'SKU-A', name: 'Alpha', available: 50 }),
      'SKU-B': stockView({ sku: 'SKU-B', name: 'Beta', available: 2, low: true }),
    });

    it('shows a table by default', () => {
      hoisted.live = live({ stock: stock() });
      render(<StockPanel />);
      expect(screen.getByRole('table')).toBeInTheDocument();
      expect(screen.queryByRole('list', { name: 'Stock levels' })).not.toBeInTheDocument();
    });

    it('shows cards instead of the table with new-inventory-dashboard on', () => {
      hoisted.live = live({
        stock: stock(),
        flags: { ...DEFAULT_UI_FLAGS, newInventoryDashboard: true },
      });
      render(<StockPanel />);
      expect(screen.queryByRole('table')).not.toBeInTheDocument();
      const cards = screen.getAllByRole('listitem');
      expect(cards).toHaveLength(2);
      expect(within(cards[0]!).getByText('Alpha')).toBeInTheDocument();
      expect(within(cards[1]!).getByText('Low')).toBeInTheDocument();
    });

    it('offers the bulk restock only with the flag on and inventory:write', () => {
      hoisted.scopes = new Set(['inventory:read', 'inventory:write']);
      hoisted.live = live({ stock: stock() });
      const { unmount } = render(<StockPanel />);
      expect(screen.queryByRole('form', { name: /restock low items/i })).not.toBeInTheDocument();
      unmount();

      hoisted.live = live({ stock: stock(), flags: { ...DEFAULT_UI_FLAGS, bulkAdjust: true } });
      const second = render(<StockPanel />);
      expect(screen.getByRole('form', { name: /restock low items/i })).toBeInTheDocument();
      expect(screen.getByRole('button', { name: 'Restock 1 low item' })).toBeInTheDocument();
      second.unmount();

      hoisted.scopes = new Set(['inventory:read']); // flag on, but the user may not write
      render(<StockPanel />);
      expect(screen.queryByRole('form', { name: /restock low items/i })).not.toBeInTheDocument();
    });
  });
});
