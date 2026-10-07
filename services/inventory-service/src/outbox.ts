import { setTimeout as sleep } from 'node:timers/promises';
import type { Envelope } from '@orderflow/contracts';
import type { EventProducer, Logger } from '@orderflow/kafka-utils';
import type { Db, Queryable } from './db.js';

/**
 * Stores an event in the outbox, inside the caller's transaction. The unique eventId makes
 * this idempotent: re-processing the same input re-derives the same eventId and adds nothing.
 */
export async function enqueue(
  tx: Queryable,
  topic: string,
  key: string,
  event: Envelope,
): Promise<void> {
  await tx.query(
    `INSERT INTO outbox (event_id, topic, key, event) VALUES ($1, $2, $3, $4::jsonb)
     ON CONFLICT (event_id) DO NOTHING`,
    [event.eventId, topic, key, JSON.stringify(event)],
  );
}

export interface OutboxRelayOptions {
  /** How often to look for new events when nothing pokes the relay. */
  pollMs?: number;
  batchSize?: number;
  /** How long published rows are kept (for debugging) before being deleted. */
  retentionMinutes?: number;
}

/**
 * Publishes outbox rows to Kafka in insertion order, then marks them published.
 *
 * Delivery is at-least-once: if the process dies after Kafka acked a record but before
 * the row is marked, the record is sent again - with the same eventId, which consumers
 * de-duplicate. Rows are claimed with `FOR UPDATE SKIP LOCKED`, so several instances can
 * run side by side; ordering across instances is then only per batch, which is why
 * stock.level.changed carries a per-SKU `version`.
 */
export class OutboxRelay {
  private readonly pollMs: number;
  private readonly batchSize: number;
  private readonly retentionMinutes: number;
  private stopped = true;
  private loop: Promise<void> | undefined;
  private wake: (() => void) | undefined;
  private poked = false;
  private lastPrune = 0;

  constructor(
    private readonly db: Db,
    private readonly producer: Pick<EventProducer, 'publish'>,
    private readonly logger: Logger,
    options: OutboxRelayOptions = {},
  ) {
    this.pollMs = options.pollMs ?? 500;
    this.batchSize = options.batchSize ?? 100;
    this.retentionMinutes = options.retentionMinutes ?? 60;
  }

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.loop = this.run();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.wake?.();
    await this.loop;
  }

  /** Wakes the relay right away (called after a transaction that wrote outbox rows). */
  poke(): void {
    this.poked = true;
    this.wake?.();
  }

  async pending(): Promise<number> {
    const { rows } = await this.db.query<{ n: string }>(
      'SELECT count(*) AS n FROM outbox WHERE published_at IS NULL',
    );
    return Number(rows[0]?.n ?? 0);
  }

  /** Publishes one batch; resolves with how many events were sent. Rejects if Kafka failed. */
  async drain(): Promise<number> {
    let failure: Error | undefined;
    const sent = await this.db.transaction(async (tx) => {
      const { rows } = await tx.query<{ id: string; topic: string; key: string; event: Envelope }>(
        `SELECT id, topic, key, event FROM outbox
         WHERE published_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`,
        [this.batchSize],
      );
      const done: string[] = [];
      for (const row of rows) {
        try {
          await this.producer.publish(row.topic, row.event, { key: row.key });
          done.push(row.id);
        } catch (err) {
          failure = err instanceof Error ? err : new Error(String(err));
          break; // keep order: nothing after a failed record may overtake it
        }
      }
      // Rows sent before a failure are still marked, so a retry doesn't resend them.
      if (done.length > 0) {
        await tx.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])', [
          done,
        ]);
      }
      return done.length;
    });
    if (failure) throw failure;
    return sent;
  }

  private async run(): Promise<void> {
    while (!this.stopped) {
      try {
        let sent: number;
        do {
          sent = await this.drain();
        } while (sent === this.batchSize && !this.stopped);
        await this.prune();
      } catch (err) {
        this.logger.warn({ err }, 'outbox relay failed, will retry');
        await sleep(1000).catch(() => undefined);
      }
      await this.idle();
    }
  }

  private idle(): Promise<void> {
    // A poke that arrived while a batch was being sent must not wait for the next poll.
    if (this.stopped || this.poked) {
      this.poked = false;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      const timer = setTimeout(done, this.pollMs);
      this.wake = done;
      function done() {
        clearTimeout(timer);
        resolve();
      }
    }).then(() => {
      this.wake = undefined;
      this.poked = false;
    });
  }

  private async prune(): Promise<void> {
    if (Date.now() - this.lastPrune < 60_000) return;
    this.lastPrune = Date.now();
    await this.db.query(
      `DELETE FROM outbox WHERE published_at < now() - make_interval(mins => $1)`,
      [this.retentionMinutes],
    );
  }
}
