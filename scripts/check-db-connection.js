/**
 * One-off: verify DATABASE_URL against the same pg config as the app.
 * Usage: node scripts/check-db-connection.js
 */
import 'dotenv/config';
import pg from 'pg';
import pgConnString from 'pg-connection-string';
import { assertDatabaseUrl, buildPoolConfigFromUrl } from '../config/database.js';

function maskUrl(url) {
  return String(url).replace(/:([^@/]+)@/, ':****@');
}

async function tryPool(label, configOrUrl) {
  const pool =
    typeof configOrUrl === 'string'
      ? new pg.Pool(buildPoolConfigFromUrl(configOrUrl))
      : new pg.Pool(configOrUrl);
  try {
    const { rows } = await pool.query('SELECT 1 AS ok, current_database() AS db');
    console.log(`[${label}] OK`, rows[0]);
    return true;
  } catch (e) {
    console.error(`[${label}] FAIL`, e.code || '', e.message);
    return false;
  } finally {
    await pool.end().catch(() => {});
  }
}

async function main() {
  const url = assertDatabaseUrl();
  console.log('Configured URL (masked):', maskUrl(url));

  const okApp = await tryPool('App config (your DATABASE_URL)', url);

  const parsed = pgConnString.parse(url);
  const supabaseHost = String(process.env.SUPABASE_URL || '')
    .replace(/^https?:\/\//i, '')
    .split('/')[0]
    .split('.')[0];
  const ref = supabaseHost || null;
  if (parsed.password != null && ref) {
    const directUrl = `postgresql://postgres:${encodeURIComponent(parsed.password)}@db.${ref}.supabase.co:5432/${parsed.database || 'postgres'}`;
    console.log('\nAlso trying direct Postgres (port 5432, user postgres)…');
    console.log('Direct URL (masked):', maskUrl(directUrl));
    await tryPool('Direct db.<ref>.supabase.co:5432', directUrl);
  }

  if (!okApp) {
    console.error('\nPooler failed — fix .env using Supabase Dashboard → Project Settings → Database → Connection string (copy URI for "Pooler" or use Direct).');
    process.exit(1);
  }
}

main().catch((e) => {
  console.error(e.message || e);
  process.exit(1);
});
