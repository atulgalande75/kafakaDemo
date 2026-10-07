import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { AdjustStock, toDelta } from './AdjustStock';

const hoisted = vi.hoisted(() => ({ mutate: vi.fn(), error: undefined as Error | undefined }));
vi.mock('../api/hooks', () => ({
  useAdjustStock: () => ({
    mutate: hoisted.mutate,
    isPending: false,
    isError: Boolean(hoisted.error),
    error: hoisted.error,
    isSuccess: false,
  }),
}));

beforeEach(() => {
  hoisted.mutate.mockClear();
  hoisted.error = undefined;
});

describe('toDelta', () => {
  it('gives the quantity the sign its reason requires', () => {
    expect(toDelta('restock', -5)).toBe(5);
    expect(toDelta('shrinkage', 5)).toBe(-5);
    expect(toDelta('correction', -2)).toBe(-2);
    expect(toDelta('correction', 2)).toBe(2);
  });
});

describe('AdjustStock', () => {
  it('submits a restock with a positive delta', async () => {
    const user = userEvent.setup();
    render(<AdjustStock skus={['SKU-A', 'SKU-B']} />);
    await user.selectOptions(screen.getByLabelText('SKU'), 'SKU-B');
    await user.clear(screen.getByLabelText('Quantity'));
    await user.type(screen.getByLabelText('Quantity'), '25');
    await user.type(screen.getByLabelText('Note'), ' supplier delivery ');
    await user.click(screen.getByRole('button', { name: 'Apply' }));
    expect(hoisted.mutate).toHaveBeenCalledWith({
      sku: 'SKU-B',
      reason: 'restock',
      delta: 25,
      note: 'supplier delivery',
    });
  });

  it('turns shrinkage into a negative delta', async () => {
    const user = userEvent.setup();
    render(<AdjustStock skus={['SKU-A']} />);
    await user.selectOptions(screen.getByLabelText('Reason'), 'shrinkage');
    await user.click(screen.getByRole('button', { name: 'Apply' }));
    expect(hoisted.mutate).toHaveBeenCalledWith({ sku: 'SKU-A', reason: 'shrinkage', delta: -10 });
  });

  it('blocks zero and fractional quantities', async () => {
    const user = userEvent.setup();
    render(<AdjustStock skus={['SKU-A']} />);
    for (const bad of ['0', '1.5']) {
      await user.clear(screen.getByLabelText('Quantity'));
      await user.type(screen.getByLabelText('Quantity'), bad);
      expect(screen.getByRole('button', { name: 'Apply' })).toBeDisabled();
    }
  });

  it('shows the error the service returned', () => {
    hoisted.error = new Error('Cannot remove 5 of SKU-A: only 2 available');
    render(<AdjustStock skus={['SKU-A']} />);
    expect(screen.getByRole('alert')).toHaveTextContent('only 2 available');
  });
});
