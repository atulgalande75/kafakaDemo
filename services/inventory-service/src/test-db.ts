import { PGlite } from '@electric-sql/pglite';
import type { Db, Queryable } from './db.js';
import { migrate } from './migrations.js';

/** A real Postgres (WASM, in-process) for tests: same SQL semantics, no Docker needed. */
export async function createTestDb(): Promise<Db & { reset(): Promise<void> }> {
  const pg = new PGlite();
  const wrap = (q: Pick<PGlite, 'query' | 'exec'>): Queryable => ({
    async query<R>(sql: string, params?: unknown[]) {
      const res = await q.query<R>(sql, params);
      return { rows: res.rows, rowCount: res.affectedRows ?? res.rows.length };
    },
    async exec(sql) {
      await q.exec(sql);
    },
  });

  const db = {
    ...wrap(pg),
    transaction: <T>(fn: (tx: Queryable) => Promise<T>) => pg.transaction((tx) => fn(wrap(tx))),
    close: () => pg.close(),
    async reset() {
      await pg.exec('TRUNCATE stock, reservations, outbox RESTART IDENTITY');
    },
  };
  await migrate(db);
  return db;
}
