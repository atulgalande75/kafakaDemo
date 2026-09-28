import type { Logger } from 'pino';

/**
 * Runs the given cleanup functions (in order) on SIGINT/SIGTERM, then exits.
 * Disconnecting consumers cleanly commits offsets and leaves the group right away
 * instead of waiting for the session timeout.
 */
export function onShutdown(logger: Logger, ...cleanups: Array<() => Promise<unknown>>): void {
  let shuttingDown = false;
  const handle = async (signal: NodeJS.Signals) => {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({ signal }, 'shutting down');
    const timer = setTimeout(() => {
      logger.error('graceful shutdown timed out');
      process.exit(1);
    }, 10_000);
    timer.unref();
    for (const cleanup of cleanups) {
      try {
        await cleanup();
      } catch (err) {
        logger.error({ err }, 'error during shutdown');
      }
    }
    process.exit(0);
  };
  process.once('SIGINT', (s) => void handle(s));
  process.once('SIGTERM', (s) => void handle(s));
}

/** Logs and exits on a fatal startup error. */
export function exitOnError(logger: Logger) {
  return (err: unknown) => {
    logger.fatal({ err }, 'fatal error');
    process.exit(1);
  };
}
