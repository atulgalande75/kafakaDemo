import { pino, type Logger } from 'pino';

export type { Logger } from 'pino';

/**
 * Creates the process logger: human-readable output for local development,
 * newline-delimited JSON when LOG_FORMAT=json or NODE_ENV=production.
 */
export function createLogger(service: string): Logger {
  const pretty = process.env.LOG_FORMAT !== 'json' && process.env.NODE_ENV !== 'production';
  return pino({
    name: service,
    level: process.env.LOG_LEVEL ?? 'info',
    ...(pretty && {
      transport: {
        target: 'pino-pretty',
        options: {
          translateTime: 'HH:MM:ss.l',
          ignore: 'pid,hostname,name',
          // stdout is a pipe under `npm run dev` (concurrently), so force colors.
          colorize: process.env.NO_COLOR === undefined,
        },
      },
    }),
  });
}
