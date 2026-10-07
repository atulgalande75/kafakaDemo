import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { OrderForm } from './OrderForm';

const hoisted = vi.hoisted(() => ({
  scopes: new Set<string>(),
  mutate: vi.fn(),
  error: undefined as Error | undefined,
}));
vi.mock('../auth/AuthProvider', () => ({
  useAuth: () => ({ username: 'alice', scopes: hoisted.scopes }),
}));
vi.mock('../api/hooks', () => ({
  useCreateOrder: () => ({
    mutate: hoisted.mutate,
    isPending: false,
    isError: Boolean(hoisted.error),
    error: hoisted.error,
    isSuccess: false,
  }),
}));

beforeEach(() => {
  hoisted.scopes = new Set(['orders:write']);
  hoisted.mutate.mockClear();
  hoisted.error = undefined;
});

describe('OrderForm', () => {
  it('places an order for the signed-in user with catalog prices', async () => {
    const user = userEvent.setup();
    render(<OrderForm />);
    await user.selectOptions(screen.getByLabelText('Product'), 'SKU-KEYBOARD');
    await user.clear(screen.getByLabelText('Qty'));
    await user.type(screen.getByLabelText('Qty'), '2');
    await user.selectOptions(screen.getByLabelText('Tier'), 'gold');
    await user.selectOptions(screen.getByLabelText('Country'), 'DE');
    await user.click(screen.getByRole('button', { name: 'Place order' }));

    expect(hoisted.mutate).toHaveBeenCalledWith({
      customerId: 'alice',
      customerTier: 'gold',
      country: 'DE',
      items: [{ sku: 'SKU-KEYBOARD', quantity: 2, unitPrice: 79.99 }],
    });
  });

  it('adds and removes lines and totals them', async () => {
    const user = userEvent.setup();
    render(<OrderForm />);
    expect(screen.queryByRole('button', { name: /remove line/i })).not.toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Add item' }));
    expect(screen.getAllByLabelText('Product')).toHaveLength(2);
    // mouse 25 + keyboard 79.99
    expect(screen.getByText(/Total .*104\.99/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Remove line 2' }));
    expect(screen.getAllByLabelText('Product')).toHaveLength(1);
  });

  it('disables submit for a bad quantity', async () => {
    const user = userEvent.setup();
    render(<OrderForm />);
    await user.clear(screen.getByLabelText('Qty'));
    await user.type(screen.getByLabelText('Qty'), '0');
    expect(screen.getByRole('button', { name: 'Place order' })).toBeDisabled();
  });

  it('warns when the total is over the card limit', async () => {
    const user = userEvent.setup();
    render(<OrderForm />);
    expect(screen.queryByText(/declined by the payment service/i)).not.toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText('Product'), 'SKU-LAPTOP');
    await user.clear(screen.getByLabelText('Qty'));
    await user.type(screen.getByLabelText('Qty'), '2');
    expect(screen.getByText(/declined by the payment service/i)).toBeInTheDocument();
  });

  it('shows the service error', () => {
    hoisted.error = new Error('Invalid order');
    render(<OrderForm />);
    expect(screen.getByRole('alert')).toHaveTextContent('Invalid order');
  });

  it('hides the form from users without orders:write', () => {
    hoisted.scopes = new Set(['orders:read']);
    render(<OrderForm />);
    expect(screen.queryByRole('form')).not.toBeInTheDocument();
    expect(screen.getByText(/not allowed to place orders/i)).toBeInTheDocument();
  });
});
