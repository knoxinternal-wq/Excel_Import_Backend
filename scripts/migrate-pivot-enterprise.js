import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import pg from 'pg';
import { assertDatabaseUrl, buildPoolConfigFromUrl } from '../config/database.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function main() {
  const sqlPath = path.join(__dirname, '../sql/pivot_enterprise.sql');
  const sql = fs.readFileSync(sqlPath, 'utf8');
  let pool;
  try {
    pool = new pg.Pool(buildPoolConfigFromUrl(assertDatabaseUrl()));
  } catch (e) {
    console.error(e.message || String(e));
    process.exit(1);
  }
  try {
    await pool.query(sql);
    console.log('pivot_enterprise.sql applied successfully.');
  } catch (e) {
    console.error('Failed:', e.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
