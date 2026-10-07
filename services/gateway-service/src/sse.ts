import type { Frame } from './hub.js';

/** Serializes a frame in the text/event-stream wire format. */
export function formatFrame(frame: Frame): string {
  const lines: string[] = [];
  if (frame.id) lines.push(`id: ${frame.id}`);
  lines.push(`event: ${frame.event}`);
  // JSON never contains a raw newline, so one data line is enough.
  lines.push(`data: ${JSON.stringify(frame.data)}`);
  return `${lines.join('\n')}\n\n`;
}

/**
 * Sent on idle connections. It is a real event (not a `:` comment) because browser-side SSE
 * libraries don't surface comments, and the client needs to *see* it to know the connection
 * is alive.
 */
export const HEARTBEAT = formatFrame({ event: 'ping', data: {} });
