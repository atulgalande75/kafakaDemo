/** Shapes returned by order-service and inventory-service (see their README sections). */
export type OrderStatus = 'PENDING' | 'CONFIRMED' | 'CANCELLED';

export interface Order {
  id: string;
  customerId: string;
  customerTier: string;
  country: string;
  items: Array<{ sku: string; quantity: number; unitPrice: number }>;
  totalAmount: number;
  currency: string;
  status: OrderStatus;
  payment: { status: 'PENDING' | 'COMPLETED' | 'FAILED'; reason?: string };
  inventory: { status: 'PENDING' | 'RESERVED' | 'REJECTED'; reason?: string };
  createdAt: string;
  updatedAt: string;
  history: Array<{ at: string; eventType: string; eventId: string; status: OrderStatus }>;
}

export interface NewOrder {
  customerId: string;
  customerTier: 'standard' | 'gold' | 'platinum';
  country: string;
  items: Array<{ sku: string; quantity: number; unitPrice: number }>;
}

export type AdjustReason = 'restock' | 'shrinkage' | 'correction';

export interface StockAdjustment {
  sku: string;
  delta: number;
  reason: AdjustReason;
  note?: string;
}
