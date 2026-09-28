import { pino, type Logger } from 'pino';

export type { Logger } from 'pino';

/**
 * Creates the process logger. Pretty, human-readable output in a terminal;
 * newline-delimited JSON when LOG_FORMAT=json or stdout is not a TTY.
 */
export function createLogger(service: string): Logger {
  const pretty = process.env.LOG_FORMAT !== 'json' && process.stdout.isTTY === true;
  return pino({
    name: service,
    level: process.env.LOG_LEVEL ?? 'info',
    ...(pretty && {
      transport: {
        target: 'pino-pretty',
        options: { translateTime: 'HH:MM:ss.l', ignore: 'pid,hostname,name' },
      },
    }),
  });
}
