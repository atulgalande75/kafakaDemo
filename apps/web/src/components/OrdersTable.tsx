import { useQueryClient } from '@tanstack/react-query';
import { Fragment, useEffect, useState } from 'react';
import { ordersKey, useOrders } from '../api/hooks';
import type { Order } from '../api/types';
import { formatMoney, formatTime, shortId } from '../format';
import { useLive } from '../live/LiveProvider';

const STATUS_CLASS: Record<Order['status'], string> = {
  PENDING: 'warn',
  CONFIRMED: 'good',
  CANCELLED: 'bad',
};

/** What each event in an order's history means, in words. */
const EVENT_LABEL: Record<string, string> = {
  'order.created': 'Order placed',
  'payment.completed': 'Payment completed',
  'payment.failed': 'Payment failed',
  'inventory.reserved': 'Stock reserved',
  'inventory.rejected': 'Stock rejected',
};

export function OrdersTable() {
  const { data: orders, isPending, error } = useOrders();
  const { orderTick } = useLive();
  const queryClient = useQueryClient();
  const [open, setOpen] = useState<string>();

  // Order status lives in order-service. The stream only says that an order moved, so refetch
  // (debounced: one order produces a burst of events).
  useEffect(() => {
    if (orderTick === 0) return;
    const timer = setTimeout(
      () => void queryClient.invalidateQueries({ queryKey: ordersKey }),
      300,
    );
    return () => clearTimeout(timer);
  }, [orderTick, queryClient]);

  return (
    <section className="card" aria-labelledby="orders-title">
      <h2 id="orders-title">My orders</h2>
      {isPending ? (
        <p className="muted">Loading…</p>
      ) : error ? (
        <p className="error" role="alert">
          {error.message}
        </p>
      ) : orders.length === 0 ? (
        <p className="muted">No orders yet. Place one above.</p>
      ) : (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Order</th>
                <th>Items</th>
                <th className="num">Total</th>
                <th>Status</th>
                <th>Payment</th>
                <th>Stock</th>
                <th>Placed</th>
              </tr>
            </thead>
            <tbody>
              {orders.map((order) => (
                <Fragment key={order.id}>
                  <tr
                    className="clickable"
                    onClick={() => setOpen(open === order.id ? undefined : order.id)}
                    aria-expanded={open === order.id}
                  >
                    <td className="mono">{shortId(order.id)}</td>
                    <td>
                      {order.items
                        .map((i) => `${i.quantity}× ${i.sku.replace('SKU-', '')}`)
                        .join(', ')}
                    </td>
                    <td className="num">{formatMoney(order.totalAmount, order.currency)}</td>
                    <td>
                      <span className={`badge ${STATUS_CLASS[order.status]}`}>{order.status}</span>
                    </td>
                    <td title={order.payment.reason}>{order.payment.status}</td>
                    <td title={order.inventory.reason}>{order.inventory.status}</td>
                    <td className="muted">{formatTime(order.createdAt)}</td>
                  </tr>
                  {open === order.id && (
                    <tr className="detail">
                      <td colSpan={7}>
                        <ol className="timeline">
                          {order.history.map((h, i) => (
                            <li key={h.eventId}>
                              <span className="muted">{formatTime(h.at)}</span>{' '}
                              {EVENT_LABEL[h.eventType] ?? h.eventType}
                              {i > 0 && h.status !== order.history[i - 1]?.status && (
                                <strong className={`outcome ${STATUS_CLASS[h.status]}`}>
                                  {' '}
                                  &rarr; order {h.status}
                                </strong>
                              )}
                            </li>
                          ))}
                        </ol>
                        <p className="hint">
                          An order is confirmed once both payment and stock have answered, and
                          cancelled as soon as either one fails.
                        </p>
                        {(order.payment.reason || order.inventory.reason) && (
                          <p className="hint">{order.payment.reason ?? order.inventory.reason}</p>
                        )}
                      </td>
                    </tr>
                  )}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
