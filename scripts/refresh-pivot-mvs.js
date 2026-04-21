import 'dotenv/config';
import pg from 'pg';
import { assertDatabaseUrl, buildPoolConfigFromUrl } from '../config/database.js';

const PIVOT_MVS = [
  'mv_sales_state_month',
  'mv_sales_branch_brand',
  'mv_sales_agent_party_month',
  'sales_mv',
];

function isConcurrentRefreshNotAllowed(err) {
  const msg = String(err?.message || '').toLowerCase();
  return msg.includes('cannot refresh materialized view') && msg.includes('concurrently');
}

async function refreshOne(pool, mvName) {
  try {
    await pool.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${mvName}`);
    console.log(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${mvName} ok`);
  } catch (err) {
    if (!isConcurrentRefreshNotAllowed(err)) throw err;
    // Fresh/unpopulated MVs cannot always use CONCURRENTLY; do one blocking refresh first.
    await pool.query(`REFRESH MATERIALIZED VIEW ${mvName}`);
    console.log(`REFRESH MATERIALIZED VIEW ${mvName} ok (fallback from CONCURRENTLY)`);
  }
}

async function main() {
  const pool = new pg.Pool(buildPoolConfigFromUrl(assertDatabaseUrl()));
  try {
    for (const mvName of PIVOT_MVS) {
      await refreshOne(pool, mvName);
    }
    console.log('Pivot MV refresh complete for all materialized views.');
  } finally {
    await pool.end();
  }
}

main().catch((e) => {
  console.error(e?.message || e);
  process.exit(1);
});
