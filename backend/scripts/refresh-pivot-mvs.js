import 'dotenv/config';
import pg from 'pg';
import { assertDatabaseUrl, buildPoolConfigFromUrl } from '../config/database.js';

async function main() {
  let pool;
  try {
    pool = new pg.Pool(buildPoolConfigFromUrl(assertDatabaseUrl()));
  } catch (e) {
    console.error(e.message || String(e));
    process.exit(1);
  }
  const names = [
    process.env.PIVOT_MV_STATE_MONTH || 'mv_sales_state_month',
    process.env.PIVOT_MV_BRANCH_BRAND || 'mv_sales_branch_brand',
    process.env.PIVOT_MV_AGENT_PARTY_MONTH || 'mv_sales_agent_party_month',
    process.env.PIVOT_MV_SALES || 'sales_mv',
  ];
  for (const n of names) {
    if (!/^[a-z_][a-z0-9_]*$/i.test(n)) {
      console.error(`Invalid materialized view name: ${n}`);
      process.exit(1);
    }
  }
  for (const mv of names) {
    try {
      await pool.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${mv}`);
      console.log(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${mv} — ok`);
    } catch (e) {
      try {
        await pool.query(`REFRESH MATERIALIZED VIEW ${mv}`);
        console.log(`REFRESH MATERIALIZED VIEW ${mv} — ok (non-concurrent)`);
      } catch (e2) {
        console.warn(`Skip ${mv}:`, e2.message || e2);
      }
    }
  }
  try {
    await pool.query('ANALYZE sales_data');
  } catch {
    /* ignore */
  } finally {
    await pool.end();
  }
}

main();
