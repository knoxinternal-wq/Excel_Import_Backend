/**
 * Why district / pin_code stay empty: they come from party_master_app keyed by TO PARTY NAME.
 * Run from backend folder: node scripts/diagnose-party-master-app.mjs
 */
import 'dotenv/config';
import { supabase } from '../models/supabase.js';
import { getPgPool, getDatabaseUrl, endPgPool } from '../config/database.js';
import { invalidateMasterCache } from '../services/masterLookupCache.js';
import { getPartyMasterAppMap } from '../services/masterLoaders.js';

async function main() {
  console.log('=== party_master_app diagnostic ===\n');

  const dbUrl = getDatabaseUrl();
  console.log('DATABASE_URL set:', Boolean(String(dbUrl || '').trim()));

  console.log('\n1) Supabase REST (anon key) — count (no row body):');
  const { count: restCount, error: eCount } = await supabase
    .from('party_master_app')
    .select('*', { count: 'exact', head: true });
  if (eCount) {
    console.log('   ERROR:', eCount.message);
    if (eCount.code) console.log('   code:', eCount.code);
    if (eCount.details) console.log('   details:', eCount.details);
    console.log('   → Fix: table exists? RLS allows SELECT for role `anon`?');
  } else {
    console.log('   count:', restCount ?? '(null)');
  }

  console.log('\n2) Supabase REST — up to 2 rows (shows JSON keys):');
  const { data: sample, error: eSample } = await supabase
    .from('party_master_app')
    .select('*')
    .limit(2);
  if (eSample) {
    console.log('   ERROR:', eSample.message);
  } else if (!sample?.length) {
    console.log('   (0 rows) — table empty, or RLS hides all rows from anon.');
  } else {
    console.log('   row[0] keys:', Object.keys(sample[0]));
    const preview = JSON.stringify(sample[0], null, 2);
    console.log('   row[0] preview:', preview.length > 600 ? `${preview.slice(0, 600)}…` : preview);
  }

  console.log('\n3) PostgreSQL via DATABASE_URL (bypasses RLS):');
  const pool = getPgPool();
  if (!pool) {
    console.log('   (skipped — no DATABASE_URL; loader cannot use Postgres path)');
  } else {
    try {
      const c = await pool.query('SELECT COUNT(*)::bigint AS n FROM party_master_app');
      const n = Number(c.rows[0]?.n ?? 0);
      console.log('   COUNT(*):', n);
      if (n > 0) {
        const one = await pool.query('SELECT * FROM party_master_app LIMIT 1');
        const row = one.rows?.[0];
        console.log('   first row keys:', row ? Object.keys(row) : []);
      }
    } catch (e) {
      console.log('   ERROR:', e?.message || String(e));
      console.log('   → Table missing, wrong schema, or DB user cannot read party_master_app.');
    }
  }

  invalidateMasterCache('master:party_master_app_v2');
  invalidateMasterCache('master:party_master_app_v1');

  console.log('\n4) Loader result (getPartyMasterAppMap) after cache clear:');
  const map = await getPartyMasterAppMap();
  console.log('   map.size (normalized party keys):', map.size);
  let shown = 0;
  for (const [k, v] of map) {
    if (shown >= 5) break;
    console.log(`   • ${k.slice(0, 72)}${k.length > 72 ? '…' : ''}`);
    console.log(`     district=${v.district ?? '(null)'}  pin_code=${v.pin_code ?? '(null)'}`);
    shown += 1;
  }
  if (map.size === 0) {
    console.log('\n   No keys loaded. Typical fixes:');
    console.log('   • Import / seed party_master_app with ACCOUNT_NAME matching TO PARTY NAME in sales.');
    console.log('   • Set DATABASE_URL so the server reads the table over Postgres (recommended).');
    console.log('   • In Supabase: Table Editor → party_master_app → RLS → policy allowing anon SELECT (if you only use REST).');
  }

  await endPgPool().catch(() => {});
  console.log('\n=== done ===');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
