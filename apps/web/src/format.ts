export const shortId = (id: string) => id.slice(0, 8);

export const formatTime = (iso: string) =>
  new Date(iso).toLocaleTimeString(undefined, { hour12: false });

export const formatMoney = (amount: number, currency = 'USD') =>
  new Intl.NumberFormat(undefined, { style: 'currency', currency }).format(amount);

/** "+5" / "−3" with a real minus sign. */
export const signed = (n: number) => (n > 0 ? `+${n}` : n < 0 ? `−${Math.abs(n)}` : '0');
