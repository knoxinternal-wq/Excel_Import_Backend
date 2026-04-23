import { getPgPool } from '../config/database.js';
import { logInfo, logWarn } from '../utils/logger.js';

export const PIVOT_MVS = Object.freeze([
  'mv_sales_all_dims',
  'mv_sales_state_month',
  'mv_sales_branch_brand',
  'mv_sales_party_grouped_brand',
  'mv_sales_state_party_grouped_brand',
  'mv_sales_agent_final_branch',
  'mv_sales_party_agent_branch',
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

/**
 * Lightweight post-refresh health check:
 * verifies latest FY present in sales_data is also present in mv_sales_all_dims.
 */
export async function checkPivotMvFreshness() {
  const pool = getPgPool();
  if (!pool) {
    logWarn('pivot_mv', 'health check skipped: no postgres pool');
    return { ok: false, skipped: true };
  }
  const exists = await pool.query('SELECT to_regclass($1)::text AS rel', ['public.mv_sales_all_dims']);
  if (!exists.rows?.[0]?.rel) {
    logWarn('pivot_mv', 'health check skipped: mv_sales_all_dims missing');
    return { ok: false, skipped: true };
  }
  const sql = `
    WITH latest_fy AS (
      SELECT fy, COUNT(1)::bigint AS src_rows
      FROM sales_data
      WHERE fy IS NOT NULL AND BTRIM(fy) <> ''
      GROUP BY fy
      ORDER BY SPLIT_PART(fy, '-', 1)::int DESC
      LIMIT 1
    ),
    mv_fy AS (
      SELECT fy, COUNT(1)::bigint AS mv_rows
      FROM mv_sales_all_dims
      WHERE fy IS NOT NULL AND BTRIM(fy) <> ''
      GROUP BY fy
    )
    SELECT l.fy, l.src_rows, COALESCE(m.mv_rows, 0) AS mv_rows
    FROM latest_fy l
    LEFT JOIN mv_fy m ON m.fy = l.fy
    LIMIT 1
  `;
  const { rows } = await pool.query(sql);
  const row = rows?.[0];
  if (!row) {
    logWarn('pivot_mv', 'health check found no FY rows in sales_data');
    return { ok: false, skipped: false, details: null };
  }
  const srcRows = Number(row.src_rows || 0);
  const mvRows = Number(row.mv_rows || 0);
  const details = { fy: row.fy, srcRows, mvRows };
  if (srcRows > 0 && mvRows === 0) {
    logWarn('pivot_mv', 'health check failed: latest FY missing in MV', details);
    return { ok: false, skipped: false, details };
  }
  logInfo('pivot_mv', 'health check ok', details);
  return { ok: true, skipped: false, details };
}
