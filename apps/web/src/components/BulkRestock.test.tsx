import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { BulkRestockResult } from '../api/hooks';
import { BulkRestock } from './BulkRestock';

const hoisted = vi.hoisted(() => ({
  mutate: vi.fn(),
  pending: false,
  data: undefined as BulkRestockResult | undefined,
}));
vi.mock('../api/hooks', () => ({
  useBulkRestock: () => ({
    mutate: hoisted.mutate,
    isPending: hoisted.pending,
    isSuccess: Boolean(hoisted.data),
    data: hoisted.data,
  }),
}));

beforeEach(() => {
  hoisted.mutate.mockClear();
  hoisted.pending = false;
  hoisted.data = undefined;
});

describe('BulkRestock', () => {
  it('restocks every low SKU by the chosen amount', async () => {
    const user = userEvent.setup();
    render(<BulkRestock lowSkus={['SKU-GPU', 'SKU-WEBCAM']} />);
    await user.clear(screen.getByLabelText('Add to each'));
    await user.type(screen.getByLabelText('Add to each'), '30');
    await user.click(screen.getByRole('button', { name: 'Restock 2 low items' }));
    expect(hoisted.mutate).toHaveBeenCalledWith({ skus: ['SKU-GPU', 'SKU-WEBCAM'], quantity: 30 });
  });

  it('is disabled when nothing is low', () => {
    render(<BulkRestock lowSkus={[]} />);
    expect(screen.getByRole('button', { name: 'Nothing is low' })).toBeDisabled();
  });

  it('refuses zero, negative and fractional amounts', async () => {
    const user = userEvent.setup();
    render(<BulkRestock lowSkus={['SKU-GPU']} />);
    for (const bad of ['0', '-5', '2.5']) {
      await user.clear(screen.getByLabelText('Add to each'));
      await user.type(screen.getByLabelText('Add to each'), bad);
      expect(screen.getByRole('button', { name: /restock 1 low item/i })).toBeDisabled();
    }
  });

  it('reports what worked and what did not', () => {
    hoisted.data = {
      restocked: ['SKU-GPU'],
      failed: [{ sku: 'SKU-WEBCAM', message: 'Unknown SKU' }],
    };
    render(<BulkRestock lowSkus={['SKU-GPU', 'SKU-WEBCAM']} />);
    expect(screen.getByText(/Restocked SKU-GPU/)).toBeInTheDocument();
    expect(screen.getByRole('alert')).toHaveTextContent('SKU-WEBCAM (Unknown SKU)');
  });
});
