import 'dotenv/config';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { migrate } from './migrate.js';

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
const directory = fileURLToPath(new URL('../migrations', import.meta.url));

const applied = await migrate(pool, directory);
console.log(applied.length ? `Applied: ${applied.join(', ')}` : 'Database is up to date');
await pool.end();
