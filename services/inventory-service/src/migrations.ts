import type { Logger } from '@orderflow/kafka-utils';
import type { Db } from './db.js';

interface Migration {
  version: number;
  name: string;
  sql: string;
}

/**
 * Append-only list of schema migrations; each runs once, in order, and is recorded in
 * `schema_migrations`. Never edit an applied migration - add a new one.
 */
export const MIGRATIONS: readonly Migration[] = [
  {
    version: 1,
    name: 'inventory-core',
    sql: `
      CREATE TABLE stock (
        sku                 text PRIMARY KEY,
        name                text NOT NULL,
        available           integer NOT NULL CHECK (available >= 0),
        low_stock_threshold integer NOT NULL DEFAULT 10 CHECK (low_stock_threshold >= 0),
        -- Bumped on every change; carried on stock.level.changed so consumers can order events.
        version             bigint NOT NULL DEFAULT 0,
        updated_at          timestamptz NOT NULL DEFAULT now()
      );

      -- One row per order: the outcome of its reservation. Makes order handling idempotent
      -- even if the same orders.created is delivered again or after stock has changed.
      --   reserved  stock was taken          rejected  not enough stock (nothing taken)
      --   released  stock was given back     cancelled payment failed before we reserved
      CREATE TABLE reservations (
        order_id       uuid PRIMARY KEY,
        reservation_id uuid NOT NULL,
        status         text NOT NULL CHECK (status IN ('reserved', 'rejected', 'released', 'cancelled')),
        items          jsonb NOT NULL DEFAULT '[]',
        detail         jsonb,
        created_at     timestamptz NOT NULL DEFAULT now(),
        released_at    timestamptz
      );

      -- Transactional outbox: events are written in the same transaction as the state change
      -- and relayed to Kafka afterwards, so a crash can never lose or invent an event.
      CREATE TABLE outbox (
        id           bigserial PRIMARY KEY,
        event_id     uuid NOT NULL UNIQUE,
        topic        text NOT NULL,
        key          text NOT NULL,
        event        jsonb NOT NULL,
        created_at   timestamptz NOT NULL DEFAULT now(),
        published_at timestamptz
      );
      CREATE INDEX outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
    `,
  },
];

const MIGRATION_LOCK_ID = 727_274;

/** Applies pending migrations. Safe to run from several instances at once. */
export async function migrate(db: Db, logger?: Logger): Promise<void> {
  await db.transaction(async (tx) => {
    await tx.query('SELECT pg_advisory_xact_lock($1)', [MIGRATION_LOCK_ID]);
    await tx.exec(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version    integer PRIMARY KEY,
        name       text NOT NULL,
        applied_at timestamptz NOT NULL DEFAULT now()
      )`);
    const { rows } = await tx.query<{ version: number }>('SELECT version FROM schema_migrations');
    const applied = new Set(rows.map((r) => Number(r.version)));

    for (const migration of MIGRATIONS) {
      if (applied.has(migration.version)) continue;
      await tx.exec(migration.sql);
      await tx.query('INSERT INTO schema_migrations (version, name) VALUES ($1, $2)', [
        migration.version,
        migration.name,
      ]);
      logger?.info({ version: migration.version, name: migration.name }, 'applied migration');
    }
  });
}
