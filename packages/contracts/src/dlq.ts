import { z } from 'zod';

/**
 * Value written to `<topic>.dlq`. It carries everything needed to understand the
 * failure *and* to replay the original message byte-for-byte (see tools/dlq-replay).
 */
export const dlqRecordSchema = z.object({
  reason: z.enum(['invalid-message', 'processing-failed']),
  error: z.object({
    name: z.string(),
    message: z.string(),
    stack: z.string().optional(),
  }),
  attempts: z.number().int().nonnegative(),
  failedAt: z.iso.datetime({ offset: true }),
  consumerGroup: z.string(),
  original: z.object({
    topic: z.string(),
    partition: z.number().int().nonnegative(),
    offset: z.string(),
    timestamp: z.string(),
    key: z.string().nullable(),
    headers: z.record(z.string(), z.string()),
    /** Original message value, utf8 if it decoded cleanly, otherwise base64. */
    value: z.string().nullable(),
    valueEncoding: z.enum(['utf8', 'base64']),
  }),
});

export type DlqRecord = z.infer<typeof dlqRecordSchema>;
