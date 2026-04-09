import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import pg from 'pg';
import { assertDatabaseUrl, buildPoolConfigFromUrl } from '../config/database.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function migrate() {
  let pool;
  try {
    pool = new pg.Pool(buildPoolConfigFromUrl(assertDatabaseUrl()));
  } catch (e) {
    if (!String(process.env.DATABASE_URL || '').trim()) {
      console.log(`
Using Supabase with anon key - migrations require DATABASE_URL (PostgreSQL connection).

Option 1: Add DATABASE_URL to .env temporarily (from Supabase Dashboard → Project Settings → Database → Connection string URI)

Option 2: Run schema manually in Supabase SQL Editor:
  - Open Supabase Dashboard → SQL Editor
  - Paste contents of backend/models/schema.sql
  - Run the script
`);
    } else {
      console.error('Migration failed:', e?.message || e);
    }
    process.exit(1);
  }
  const schemaPath = path.join(__dirname, '../models/schema.sql');
  const schema = fs.readFileSync(schemaPath, 'utf8');

  try {
    await pool.query(schema);
    console.log('Migration completed successfully.');
  } catch (err) {
    console.error('Migration failed:', err.message);
  } finally {
    await pool.end();
  }
}

migrate();
