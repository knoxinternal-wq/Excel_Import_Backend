import { getPgPool } from '../config/database.js';
import { logInfo, logWarn } from '../utils/logger.js';

export const PIVOT_MVS = Object.freeze([
  'mv_sales_state_month',
  'mv_sales_branch_brand',
  'mv_sales_party_grouped_brand',
  'mv_sales_state_party_grouped_brand',
  'mv_sales_agent_party_month',
  'sales_mv',
]);

function isConcurrentRefreshNotAllowed(err) {
  const msg = String(err?.message || '').toLowerCase();
  return msg.includes('cannot refresh materialized view') && msg.includes('concurrently');
}

export async function refreshPivotMVs() {
  const pool = getPgPool();
  if (!pool) {
    logWarn('pivot_mv', 'refresh skipped: no postgres pool');
    return { refreshed: [], skipped: true };
  }
  const refreshed = [];
  for (const mvName of PIVOT_MVS) {
    // Skip safely when schema migration for a new MV is not applied yet.
    // This keeps imports/cron healthy during rolling deploys.
    // eslint-disable-next-line no-await-in-loop
    const exists = await pool.query('SELECT to_regclass($1)::text AS rel', [`public.${mvName}`]);
    if (!exists.rows?.[0]?.rel) {
      logWarn('pivot_mv', 'refresh skipped: relation missing', { mv: mvName });
      continue;
    }
    try {
      // eslint-disable-next-line no-await-in-loop
      await pool.query(`REFRESH MATERIALIZED VIEW CONCURRENTLY ${mvName}`);
      refreshed.push({ mv: mvName, mode: 'concurrent' });
      logInfo('pivot_mv', 'refresh ok', { mv: mvName, mode: 'concurrent' });
    } catch (err) {
      if (!isConcurrentRefreshNotAllowed(err)) throw err;
      // Fresh/unpopulated MVs may not support CONCURRENTLY; run one blocking refresh.
      // eslint-disable-next-line no-await-in-loop
      await pool.query(`REFRESH MATERIALIZED VIEW ${mvName}`);
      refreshed.push({ mv: mvName, mode: 'blocking' });
      logWarn('pivot_mv', 'refresh fallback blocking', { mv: mvName });
    }
  }
  return { refreshed, skipped: false };
}
