import pg from 'pg';

export interface QueryResult<R> {
  rows: R[];
  rowCount: number;
}

export interface Queryable {
  query<R = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<R>>;
  /** Runs one or more statements without parameters (migrations). */
  exec(sql: string): Promise<void>;
}

/** The slice of Postgres the service needs: queries and transactions. */
export interface Db extends Queryable {
  /** Runs `fn` in a transaction: commits when it resolves, rolls back when it throws. */
  transaction<T>(fn: (tx: Queryable) => Promise<T>): Promise<T>;
  close(): Promise<void>;
}

export const DEFAULT_DATABASE_URL = 'postgres://orderflow:orderflow@localhost:5432/orderflow';

export function createPgDb(
  connectionString = process.env.DATABASE_URL?.trim() || DEFAULT_DATABASE_URL,
): Db {
  const pool = new pg.Pool({ connectionString, max: 10 });

  const wrap = (q: pg.Pool | pg.PoolClient): Queryable => ({
    async query<R>(sql: string, params?: unknown[]) {
      const res = await q.query(sql, params);
      return { rows: res.rows as R[], rowCount: res.rowCount ?? 0 };
    },
    async exec(sql) {
      await q.query(sql);
    },
  });

  return {
    ...wrap(pool),
    async transaction(fn) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');
        const result = await fn(wrap(client));
        await client.query('COMMIT');
        return result;
      } catch (err) {
        await client.query('ROLLBACK').catch(() => undefined);
        throw err;
      } finally {
        client.release();
      }
    },
    close: () => pool.end(),
  };
}

/** Hostname-less description of a connection string, safe to log (no password). */
export function describeDatabase(connectionString: string): string {
  try {
    const url = new URL(connectionString);
    return `${url.hostname}:${url.port || '5432'}${url.pathname}`;
  } catch {
    return 'database';
  }
}
