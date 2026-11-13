import 'dotenv/config';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { migrate } from '../src/index.js';

export const silentLogger = { warn() {}, error() {} };

export function createPool() {
  return new pg.Pool({
    connectionString: process.env.DATABASE_URL ?? 'postgresql://app:app@localhost:5432/events',
    max: 20,
  });
}

/** Recreates the schema so every test starts from an empty database. */
export async function resetDatabase(pool: pg.Pool) {
  await pool.query(`
    DROP TABLE IF EXISTS processed_events, outbox_events, schema_migrations, orders, inventory CASCADE
  `);
  await migrate(pool, fileURLToPath(new URL('../migrations', import.meta.url)));
  await pool.query(
    await readFile(new URL('../examples/orders/schema.sql', import.meta.url), 'utf8'),
  );
}

export async function eventRow(pool: pg.Pool, id: string) {
  const { rows } = await pool.query('SELECT * FROM outbox_events WHERE id = $1', [id]);
  return rows[0];
}

/** Makes every pending event due now, so retry tests do not have to wait. */
export async function fastForward(pool: pg.Pool) {
  await pool.query('UPDATE outbox_events SET available_at = now() WHERE processed_at IS NULL');
}
