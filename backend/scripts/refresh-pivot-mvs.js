import 'dotenv/config';
import pg from 'pg';
import { assertDatabaseUrl, buildPoolConfigFromUrl } from '../config/database.js';

async function main() {
  const pool = new pg.Pool(buildPoolConfigFromUrl(assertDatabaseUrl()));
  try {
    await pool.query('REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sales_state_month');
    await pool.query('REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sales_branch_brand');
    await pool.query('REFRESH MATERIALIZED VIEW CONCURRENTLY mv_sales_agent_party_month');
    await pool.query('REFRESH MATERIALIZED VIEW CONCURRENTLY sales_mv');
    console.log('REFRESH MATERIALIZED VIEW CONCURRENTLY — all four MVs ok');
  } finally {
    await pool.end();
  }
}

main().catch((e) => {
  console.error(e?.message || e);
  process.exit(1);
});
