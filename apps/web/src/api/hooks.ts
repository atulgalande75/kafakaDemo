import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useAuth } from '../auth/AuthProvider';
import { api } from './client';
import type { NewOrder, Order, StockAdjustment } from './types';

export const ordersKey = ['orders'] as const;

/** The signed-in user's most recent orders (admins would get everyone's). */
export function useOrders() {
  const { getAccessToken } = useAuth();
  return useQuery({
    queryKey: ordersKey,
    queryFn: ({ signal }) =>
      api<Order[]>(getAccessToken, 'order-service', '/orders?limit=30', { signal }),
  });
}

export function useCreateOrder() {
  const { getAccessToken } = useAuth();
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (order: NewOrder) =>
      api<Order>(getAccessToken, 'order-service', '/orders', { method: 'POST', body: order }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ordersKey }),
  });
}

/** Stock levels themselves arrive over the stream; this only sends the change. */
export function useAdjustStock() {
  const { getAccessToken } = useAuth();
  return useMutation({
    mutationFn: ({ sku, ...body }: StockAdjustment) =>
      api<unknown>(
        getAccessToken,
        'inventory-service',
        `/inventory/${encodeURIComponent(sku)}/adjust`,
        {
          method: 'POST',
          body,
        },
      ),
  });
}

export interface BulkRestockResult {
  restocked: string[];
  failed: Array<{ sku: string; message: string }>;
}

/** Restocks several SKUs at once: one API call each, so one failure doesn't stop the others. */
export function useBulkRestock() {
  const { getAccessToken } = useAuth();
  return useMutation({
    mutationFn: async ({
      skus,
      quantity,
    }: {
      skus: string[];
      quantity: number;
    }): Promise<BulkRestockResult> => {
      const results = await Promise.allSettled(
        skus.map((sku) =>
          api<unknown>(
            getAccessToken,
            'inventory-service',
            `/inventory/${encodeURIComponent(sku)}/adjust`,
            {
              method: 'POST',
              body: { delta: quantity, reason: 'restock', note: 'bulk restock of low items' },
            },
          ),
        ),
      );
      const outcome: BulkRestockResult = { restocked: [], failed: [] };
      results.forEach((result, i) => {
        const sku = skus[i]!;
        if (result.status === 'fulfilled') outcome.restocked.push(sku);
        else {
          const reason = result.reason as unknown;
          outcome.failed.push({
            sku,
            message: reason instanceof Error ? reason.message : String(reason),
          });
        }
      });
      return outcome;
    },
  });
}
