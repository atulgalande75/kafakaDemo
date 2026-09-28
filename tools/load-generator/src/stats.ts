export function percentile(values: number[], p: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.max(0, index)]!;
}

export interface SettledOrder {
  status: string;
  createdAt: string;
  updatedAt: string;
  payment: { status: string; reason?: string };
  inventory: { status: string; reason?: string };
}

export interface Summary {
  total: number;
  byStatus: Record<string, number>;
  cancelReasons: Record<string, number>;
  latencyMs: { p50: number; p95: number; max: number };
}

/** Aggregates final order states: counts, cancellation reasons and end-to-end latency. */
export function summarize(orders: SettledOrder[]): Summary {
  const byStatus: Record<string, number> = {};
  const cancelReasons: Record<string, number> = {};
  const latencies: number[] = [];

  for (const order of orders) {
    byStatus[order.status] = (byStatus[order.status] ?? 0) + 1;
    if (order.status === 'CANCELLED') {
      const reason = order.payment.reason ?? order.inventory.reason ?? 'unknown';
      // Group "Amount 2998 USD exceeds..." style reasons together.
      const key = reason.replace(/\d+(\.\d+)?/g, 'N');
      cancelReasons[key] = (cancelReasons[key] ?? 0) + 1;
    }
    if (order.status !== 'PENDING') {
      latencies.push(Date.parse(order.updatedAt) - Date.parse(order.createdAt));
    }
  }

  return {
    total: orders.length,
    byStatus,
    cancelReasons,
    latencyMs: {
      p50: percentile(latencies, 50),
      p95: percentile(latencies, 95),
      max: latencies.length ? Math.max(...latencies) : 0,
    },
  };
}
