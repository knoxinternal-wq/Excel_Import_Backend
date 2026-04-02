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
  const mv = process.env.PIVOT_MV_BRAND_STATE_MONTH || 'mv_sales_brand_state_month';
  if (!/^[a-z_][a-z0-9_]*$/i.test(mv)) {
    console.error('Invalid PIVOT_MV_BRAND_STATE_MONTH');
    process.exit(1);
  }
  try {
    await pool.query(`REFRESH MATERIALIZED VIEW ${mv}`);
    console.log(`REFRESH MATERIALIZED VIEW ${mv} — ok`);
  } catch (e) {
    console.error(e.message);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
