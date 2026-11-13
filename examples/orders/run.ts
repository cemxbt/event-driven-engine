import 'dotenv/config';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { Dispatcher, OutboxWorker, listDeadLetters, migrate } from '../../src/index.js';
import { placeOrder, registerOrderConsumers } from './domain.js';

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
await migrate(pool, fileURLToPath(new URL('../../migrations', import.meta.url)));
await pool.query(await readFile(new URL('./schema.sql', import.meta.url), 'utf8'));
await pool.query(`INSERT INTO inventory (sku, available) VALUES ('BOOK-1', 3)
                  ON CONFLICT (sku) DO UPDATE SET available = 3`);

const sent: string[] = [];
const mailer = { send: async (to: string, subject: string) => void sent.push(`${to}: ${subject}`) };

// Two workers compete for the same queue, as two service instances would.
const workers = [1, 2].map(() => {
  const dispatcher = registerOrderConsumers(
    new Dispatcher(pool, { maxAttempts: 3, baseDelayMs: 200, logger: { warn() {}, error() {} } }),
    mailer,
  );
  return new OutboxWorker(pool, dispatcher, { pollIntervalMs: 200 });
});
await Promise.all(workers.map((w) => w.start()));

for (let i = 1; i <= 4; i++) {
  await placeOrder(pool, { sku: 'BOOK-1', quantity: 1, email: `customer${i}@example.com` });
}

await new Promise((resolve) => setTimeout(resolve, 2_500));
await Promise.all(workers.map((w) => w.stop()));

const { rows: orders } = await pool.query('SELECT email, status FROM orders ORDER BY created_at');
console.table(orders);
console.log('Emails sent:', sent.length);
console.log(
  'Dead letters:',
  (await listDeadLetters(pool)).map((d) => d.lastError),
);
await pool.end();
