/**
 * PostgreSQL-side pivot aggregation and drilldown (optional).
 * Uses shared `DATABASE_URL` pool from `config/database.js`. On failure, callers fall back to Node streaming.
 */
import crypto from 'crypto';
import { getPgPool } from '../config/database.js';
import { logDebug } from '../utils/logger.js';
import {
  isPivotRedisConfigured,
  pivotFilterValuesRedisGet,
  pivotFilterValuesRedisSet,
} from './pivotRedisCache.js';
import { SALES_DATA_NUMERIC_COLUMNS_SET, parseFactNumeric } from '../utils/salesFacts.js';

const SALES_FIELDS = [
  'id',
  'branch', 'fy', 'month', 'mmm', 'region', 'state', 'district', 'city',
  'business_type', 'agent_names_correction', 'party_grouped', 'party_name_for_count',
  'brand', 'agent_name', 'to_party_name',
  'bill_no', 'bill_date',
  'item_no', 'shade_name',
  'rate_unit', 'size', 'units_pack', 'sl_qty',
  'gross_amount', 'amount_before_tax', 'net_amount',
  'sale_order_no', 'sale_order_date',
  'item_with_shade', 'item_category', 'item_sub_cat', 'so_type', 'scheme',
  'goods_type', 'agent_name_final', 'pin_code',
  'created_at',
];

/** Measures only — `id` is excluded so UUID/bigint keys are never cast to numeric (avoids "invalid input syntax for type uuid"). */
const NUMERIC_FIELDS = SALES_DATA_NUMERIC_COLUMNS_SET;

const DATE_FIELDS = new Set(['bill_date', 'sale_order_date', 'created_at']);
const PARTY_EQ_FIELDS = new Set(['to_party_name', 'party_grouped', 'party_name_for_count']);

/** Guardrail for scalable pivot fan-out; cap with PIVOT_MAX_GROUP_DIMENSIONS=2..8. */
function getMaxGroupDimensions() {
  const n = Number(process.env.PIVOT_MAX_GROUP_DIMENSIONS);
  if (Number.isFinite(n) && n >= 2 && n <= 8) return Math.floor(n);
  return 5;
}

const MAX_VALUE_SPECS = 12;
const FILTER_VALUES_CACHE_TTL_MS = Number(process.env.PIVOT_FILTER_VALUES_CACHE_TTL_MS) || 60 * 60 * 1000;
const FILTER_VALUES_CACHE_MAX = Number(process.env.PIVOT_FILTER_VALUES_CACHE_MAX) || 100;
const filterValuesCache = new Map();
const SALES_ALL_DIMS_MV_NAME = process.env.PIVOT_MV_ALL_DIMS || 'mv_sales_all_dims';
const ALL_DIMS_MV_FIELDS = new Set([
  'branch',
  'region',
  'state',
  'district',
  'city',
  'brand',
  'party_grouped',
  'agent_name',
  'fy',
  'month',
  'mmm',
  'so_type',
  'item_category',
  'item_sub_cat',
  'goods_type',
]);

/**
 * Per-statement timeout for pivot **aggregation** (GROUP BY / MV paths).
 * - Local default: 5s (fail fast while developing).
 * - Production default (`NODE_ENV=production` and unset): 3 minutes — large fact tables need more time on Render etc.
 * - Override: `PIVOT_PG_STATEMENT_TIMEOUT_MS` (ms) or `PIVOT_SLA_MS`; `0` = disable (not recommended on shared DBs).
 */
function isProductionLikeHost() {
  if (String(process.env.NODE_ENV || '').toLowerCase() === 'production') return true;
  // Render.com sets RENDER=true; pivot needs more than 5s on large tables if PIVOT_PG_STATEMENT_TIMEOUT_MS is unset.
  if (String(process.env.RENDER || '').toLowerCase() === 'true') return true;
  return false;
}

function defaultPivotAggregationTimeoutMs() {
  return isProductionLikeHost() ? 180_000 : 5000;
}

function getPivotAggregationTimeoutMs() {
  const raw = process.env.PIVOT_PG_STATEMENT_TIMEOUT_MS;
  if (raw !== undefined && raw !== '') {
    const n = Number(raw);
    if (!Number.isFinite(n) || n < 0) return defaultPivotAggregationTimeoutMs();
    return Math.floor(n);
  }
  const sla = process.env.PIVOT_SLA_MS;
  if (sla !== undefined && sla !== '') {
    const n = Number(sla);
    if (!Number.isFinite(n) || n < 0) return defaultPivotAggregationTimeoutMs();
    return Math.floor(n);
  }
  return defaultPivotAggregationTimeoutMs();
}

/**
 * DISTINCT filter lists + drilldown SQL — separate from pivot aggregation timeout.
 * Default 3 min so large `sales_data` scans finish; client axios should stay ≥ this (see frontend timing.js).
 * Override with `PIVOT_FILTER_SQL_TIMEOUT_MS` (ms); `0` = off.
 */
function getPivotSupportingSqlTimeoutMs() {
  const raw = process.env.PIVOT_FILTER_SQL_TIMEOUT_MS;
  if (raw !== undefined && raw !== '') {
    const n = Number(raw);
    if (!Number.isFinite(n) || n < 0) return 180_000;
    return Math.floor(n);
  }
  return 180_000;
}

function resolveStatementTimeoutMs(options = {}) {
  if (options.timeoutMs !== undefined && options.timeoutMs !== null) {
    const n = Math.floor(Number(options.timeoutMs));
    if (!Number.isFinite(n) || n < 0) return getPivotAggregationTimeoutMs();
    return n;
  }
  return getPivotAggregationTimeoutMs();
}

/** Larger hash aggregates for million-row GROUP BY (e.g. PIVOT_PG_WORK_MEM=256MB). */
function pivotSessionWorkMemSql() {
  const raw = String(process.env.PIVOT_PG_WORK_MEM || '').trim();
  if (!raw) return null;
  const compact = raw.replace(/\s+/g, '').toUpperCase();
  if (!/^\d+(KB|MB|GB|TB)$/.test(compact)) return null;
  const escaped = compact.replace(/'/g, "''");
  return `SET LOCAL work_mem = '${escaped}'`;
}

/** Parallel hash aggregate / seq scan (0–8). Empty env = leave server default. */
function pivotSessionParallelGatherSql() {
  const raw = String(process.env.PIVOT_PG_PARALLEL_WORKERS || '').trim();
  if (raw === '') return null;
  const n = Math.floor(Number(raw));
  if (!Number.isFinite(n) || n < 0 || n > 8) return null;
  return `SET LOCAL max_parallel_workers_per_gather = ${n}`;
}

/**
 * Run pivot SQL on one connection with SET LOCAL statement_timeout (each statement in `runner` gets this cap).
 * @param {(client: import('pg').PoolClient) => Promise<unknown>} runner
 * @param {{ timeoutMs?: number }} [options] — omit to use pivot aggregation default; pass supporting timeout for filters/drilldown.
 */
async function withPivotSqlClient(runner, options = {}) {
  const p = getPivotSqlPool();
  if (!p) throw new Error('No database pool');
  const client = await p.connect();
  const ms = resolveStatementTimeoutMs(options);
  try {
    await client.query('BEGIN');
    if (ms === 0) {
      await client.query('SET LOCAL statement_timeout = 0');
    } else {
      await client.query(`SET LOCAL statement_timeout = '${ms}ms'`);
    }
    const wm = pivotSessionWorkMemSql();
    if (wm) await client.query(wm);
    const par = pivotSessionParallelGatherSql();
    if (par) await client.query(par);
    const out = await runner(client);
    await client.query('COMMIT');
    return out;
  } catch (e) {
    try {
      await client.query('ROLLBACK');
    } catch {
      /* ignore */
    }
    throw e;
  } finally {
    client.release();
  }
}

/** Postgres cancel (statement_timeout) — map to HTTP 504 + PIVOT_TIMEOUT in controllers. */
export function isPivotSqlStatementTimeoutError(err) {
  if (!err) return false;
  if (err.code === '57014') return true;
  const m = String(err.message || '');
  return /canceling statement due to statement timeout|statement timeout/i.test(m);
}

/**
 * Sum of per-group row counts from the GROUP BY result equals COUNT(*) over the same filter (each fact row maps to one group).
 */
function filteredRowCountFromGroupRows(sqlRows, values) {
  if (!sqlRows?.length) return 0;
  const specs = values || [];
  let col = null;
  for (let vi = 0; vi < specs.length; vi += 1) {
    const a = String(specs[vi]?.agg || '').toLowerCase();
    if (a === 'count') {
      col = `agg_cnt_${vi}`;
      break;
    }
    col = `agg_rowcnt_${vi}`;
    break;
  }
  if (!col) return 0;
  return sqlRows.reduce((s, r) => s + (Number(r[col]) || 0), 0);
}

export function getPivotSqlPool() {
  return getPgPool();
}

export function getConfiguredPivotMaxGroupDimensions() {
  return getMaxGroupDimensions();
}

function quoteIdent(field) {
  if (!SALES_FIELDS.includes(field)) throw new Error(`Invalid column: ${field}`);
  return `"${String(field).replace(/"/g, '""')}"`;
}

function derivedTemporalAxisExpr(field, tableAlias = 'sd') {
  const fieldCol = `${tableAlias}.${quoteIdent(field)}`;
  const billDateCol = `${tableAlias}.${quoteIdent('bill_date')}`;
  const monthCol = `${tableAlias}.${quoteIdent('month')}`;
  const monthText = `BTRIM(${monthCol}::text)`;
  const monthMatches = `${monthText} ~* '^[A-Za-z]{3,9}\\s*[-/]\\s*\\d{2,4}$'`;
  const monthToken = `LOWER(LEFT(${monthText}, 3))`;
  const monthNumFromText = `(
    CASE ${monthToken}
      WHEN 'jan' THEN 1
      WHEN 'feb' THEN 2
      WHEN 'mar' THEN 3
      WHEN 'apr' THEN 4
      WHEN 'may' THEN 5
      WHEN 'jun' THEN 6
      WHEN 'jul' THEN 7
      WHEN 'aug' THEN 8
      WHEN 'sep' THEN 9
      WHEN 'oct' THEN 10
      WHEN 'nov' THEN 11
      WHEN 'dec' THEN 12
      ELSE NULL
    END
  )`;
  const monthYearText = `SUBSTRING(${monthText} FROM '(\\d{2}|\\d{4})$')`;
  const monthYear = `(
    CASE
      WHEN LENGTH(${monthYearText}) = 2 THEN 2000 + (${monthYearText})::int
      WHEN LENGTH(${monthYearText}) = 4 THEN (${monthYearText})::int
      ELSE NULL
    END
  )`;
  if (field === 'fy') {
    return `(
      CASE
        WHEN ${fieldCol} IS NOT NULL AND BTRIM(${fieldCol}::text) <> '' THEN BTRIM(${fieldCol}::text)
        WHEN ${billDateCol} IS NOT NULL THEN (
          CASE
            WHEN EXTRACT(MONTH FROM ${billDateCol}::date) >= 4
              THEN CONCAT(EXTRACT(YEAR FROM ${billDateCol}::date)::int, '-', RIGHT((EXTRACT(YEAR FROM ${billDateCol}::date)::int + 1)::text, 2))
            ELSE CONCAT((EXTRACT(YEAR FROM ${billDateCol}::date)::int - 1), '-', RIGHT(EXTRACT(YEAR FROM ${billDateCol}::date)::int::text, 2))
          END
        )
        WHEN ${monthCol} IS NOT NULL AND ${monthMatches} AND ${monthNumFromText} IS NOT NULL AND ${monthYear} IS NOT NULL THEN (
          CASE
            WHEN ${monthNumFromText} >= 4
              THEN CONCAT(${monthYear}, '-', RIGHT((${monthYear} + 1)::text, 2))
            ELSE CONCAT((${monthYear} - 1), '-', RIGHT(${monthYear}::text, 2))
          END
        )
        ELSE NULL
      END
    )`;
  }
  if (field === 'month') {
    return `(
      CASE
        WHEN ${fieldCol} IS NOT NULL AND BTRIM(${fieldCol}::text) <> '' THEN BTRIM(${fieldCol}::text)
        WHEN ${billDateCol} IS NOT NULL THEN TO_CHAR(${billDateCol}::date, 'Mon-YY')
        ELSE NULL
      END
    )`;
  }
  if (field === 'mmm') {
    return `(
      CASE
        WHEN ${fieldCol} IS NOT NULL AND BTRIM(${fieldCol}::text) <> '' THEN BTRIM(${fieldCol}::text)
        WHEN ${billDateCol} IS NOT NULL THEN UPPER(TO_CHAR(${billDateCol}::date, 'Mon'))
        ELSE NULL
      END
    )`;
  }
  return null;
}

function normText(v) {
  return String(v ?? '').trim().toLowerCase();
}

function escapeIlike(s) {
  return String(s ?? '')
    .replace(/\\/g, '\\\\')
    .replace(/%/g, '\\%')
    .replace(/_/g, '\\_');
}

/**
 * Fast DISTINCT for pivot filter dropdowns (avoids scanning the full table via Supabase pages).
 * @returns {Promise<string[]|null>} null if no DB pool (caller falls back to stream scan).
 */
export async function queryDistinctPivotFilterValues(field, search = '', limit = '') {
  const p = getPivotSqlPool();
  if (!p) return null;
  const clean = String(field || '').trim();
  if (!ALL_DIMS_MV_FIELDS.has(clean)) throw new Error('Invalid filter field');
  const col = quoteIdent(clean);
  const lim = Math.min(20_000, Math.max(1, Math.floor(Number(limit) || 10_000)));
  const term = String(search || '').trim();
  const cacheKey = JSON.stringify({ field: clean, search: term.toLowerCase(), limit: lim });
  const cached = filterValuesCache.get(cacheKey);
  if (cached && Date.now() - cached.ts < FILTER_VALUES_CACHE_TTL_MS) {
    return cached.values;
  }

  const redisKeyHash = crypto.createHash('sha256').update(cacheKey).digest('hex');
  if (isPivotRedisConfigured()) {
    try {
      const rHit = await pivotFilterValuesRedisGet(redisKeyHash);
      if (rHit && Array.isArray(rHit.values)) {
        filterValuesCache.set(cacheKey, { ts: Date.now(), values: rHit.values });
        return rHit.values;
      }
    } catch {
      /* ignore */
    }
  }

  const values = await withPivotSqlClient(async (client) => {
    const params = [];
    let extra = '';
    if (term) {
      params.push(`%${escapeIlike(term)}%`);
      extra = ` AND ${col}::text ILIKE $1 ESCAPE '\\'`;
    }
    const mvRel = quoteMvName(SALES_ALL_DIMS_MV_NAME);
    // DISTINCT from pre-aggregated MV dims avoids scanning full `sales_data`.
    const sql = `
      SELECT DISTINCT BTRIM(${col}::text) AS v
      FROM ${mvRel}
      WHERE ${col} IS NOT NULL AND BTRIM(${col}::text) <> ''
      ${extra}
      ORDER BY 1
      LIMIT ${lim}
    `;
    const { rows } = await client.query(sql, params);
    return rows
      .map((r) => String(r?.v ?? '').trim())
      .filter(Boolean);
  }, { timeoutMs: getPivotSupportingSqlTimeoutMs() });

  if (isPivotRedisConfigured()) {
    void pivotFilterValuesRedisSet(redisKeyHash, { values });
  }
  filterValuesCache.set(cacheKey, { ts: Date.now(), values });
  while (filterValuesCache.size > FILTER_VALUES_CACHE_MAX) {
    const oldestKey = filterValuesCache.keys().next().value;
    filterValuesCache.delete(oldestKey);
  }
  return values;
}

/**
 * True when GROUP BY on sales_data columns matches axisValueFromRow.
 * Agent display fields are expected to be populated at import / DB backfill (no pivot-time master map).
 */
export function isPivotSqlAggregationEligible(normalized, memFilters) {
  if (memFilters?.length) return false;
  const pool = getPivotSqlPool();
  if (!pool) return false;
  const { rows, columns, values } = normalized;
  if (rows.length + columns.length > getMaxGroupDimensions()) return false;
  if (values.length === 0 || values.length > MAX_VALUE_SPECS) return false;
  const dims = [...rows, ...columns];
  if (dims.some((d) => !ALL_DIMS_MV_FIELDS.has(d))) return false;
  const filters = normalized.filters || [];
  if (filters.some((f) => !f?.field || !ALL_DIMS_MV_FIELDS.has(f.field))) return false;
  if (filters.some((f) => !['eq', 'in', 'contains', 'is_blank', 'is_not_blank'].includes(String(f.operator || 'eq').toLowerCase()))) return false;
  for (const v of values) {
    const agg = String(v.agg || '').toLowerCase();
    const field = v.field;
    if (!['sum', 'count'].includes(agg)) return false;
    if (agg === 'sum' && !['net_amount', 'sl_qty'].includes(field)) return false;
  }
  return true;
}

export function isPivotSqlDrilldownEligible(normalized, memFilters) {
  if (memFilters?.length) return false;
  return !!getPivotSqlPool();
}

function appendWhere(sqlParts, params, fragment) {
  sqlParts.push(fragment);
}

function grandTotalBranchExclusionSql(tableAlias = 'sd') {
  const branchCol = `${tableAlias}.${quoteIdent('branch')}`;
  return `NOT (
    LOWER(BTRIM(COALESCE(${branchCol}::text, ''))) = 'total'
    OR LOWER(BTRIM(COALESCE(${branchCol}::text, ''))) = 'grand total'
    OR LOWER(BTRIM(COALESCE(${branchCol}::text, ''))) LIKE '%grand total%'
    OR LOWER(BTRIM(COALESCE(${branchCol}::text, ''))) LIKE '%grandtotal%'
  )`;
}

function quoteMvName(name) {
  const n = String(name || '').trim();
  if (!/^[a-z_][a-z0-9_]*$/i.test(n)) throw new Error('Invalid relation name');
  return `"${n.replace(/"/g, '""')}"`;
}

function partyFilterExpr(tableAlias, field) {
  const col = `${tableAlias}.${quoteIdent(field)}`;
  if (field === 'to_party_name') {
    // New schema exposes indexed normalized party key on fact/master tables.
    return `COALESCE(${tableAlias}."norm_party", ${col}::text)`;
  }
  return col;
}

/**
 * SQL: fact column → numeric with comma stripping (aligned with `parseFactNumeric` / Excel-style values).
 */
function numericCoerceExpr(tableAlias, field) {
  const col = `${tableAlias}.${quoteIdent(field)}`;
  if (field === 'units_pack') {
    const cleaned = `trim(replace(${col}::text, ',', ''))`;
    return `(CASE WHEN ${col} IS NULL OR trim(${col}::text) = '' THEN NULL WHEN ${cleaned} ~ '^[-+]?[0-9]+([.][0-9]*)?$' THEN ${cleaned}::numeric ELSE NULL END)`;
  }
  const stripped = `NULLIF(regexp_replace(BTRIM(${col}::text), ',', '', 'g'), '')`;
  return `(CASE WHEN ${col} IS NULL THEN NULL ELSE ${stripped}::numeric END)`;
}

/**
 * Pivot config matches pre-built materialized view: rows Brand→State→Month, sum(net_amount), no column dims.
 * Filters (if any) must only reference brand, state, or month.
 */
export function matchBrandStateMonthMaterializedView(normalized, sqlFilters) {
  const r = normalized.rows || [];
  if (r.length !== 3 || r[0] !== 'brand' || r[1] !== 'state' || r[2] !== 'month') return false;
  if ((normalized.columns || []).length !== 0) return false;
  const v = normalized.values || [];
  if (v.length !== 1) return false;
  if (String(v[0].agg || '').toLowerCase() !== 'sum' || v[0].field !== 'net_amount') return false;
  for (const f of sqlFilters || []) {
    if (!f?.field) continue;
    // MV has no id; merged pivot filters may include id → use live aggregation instead.
    if (!['brand', 'state', 'month'].includes(f.field)) return false;
  }
  return true;
}

function buildWhereFromSqlFilters(sqlFilters, tableAlias = 'sd') {
  const parts = [grandTotalBranchExclusionSql(tableAlias)];
  const params = [];
  let i = 1;

  const next = (v) => {
    params.push(v);
    return `$${i++}`;
  };

  for (const f of sqlFilters || []) {
    if (!f || !f.field || !SALES_FIELDS.includes(f.field)) continue;
    const col = `${tableAlias}.${quoteIdent(f.field)}`;
    const op = String(f.operator || 'eq').toLowerCase();

    // Primary keys may be UUID or bigint — never compare via ::numeric or uuid casts of "0".
    if (f.field === 'id') {
      if (op === 'is_blank') {
        appendWhere(parts, params, `(TRIM(COALESCE(${col}::text, '')) = '')`);
        continue;
      }
      if (op === 'is_not_blank') {
        appendWhere(parts, params, `(TRIM(COALESCE(${col}::text, '')) <> '')`);
        continue;
      }
      if (op === 'in') {
        const vals = Array.isArray(f.values) ? f.values : [];
        const cleaned = vals.map((v) => String(v ?? '').trim()).filter(Boolean);
        if (!cleaned.length) continue;
        const ph = next(cleaned);
        appendWhere(parts, params, `(TRIM(COALESCE(${col}::text, '')) = ANY(${ph}::text[]))`);
        continue;
      }
      if (op === 'eq') {
        const v = String(f.value ?? '').trim();
        if (v === '') {
          appendWhere(parts, params, `(TRIM(COALESCE(${col}::text, '')) = '')`);
          continue;
        }
        const ph = next(v);
        appendWhere(parts, params, `(TRIM(COALESCE(${col}::text, '')) = TRIM(${ph}::text))`);
        continue;
      }
      if (op === 'contains') {
        const inner = escapeIlike(String(f.value ?? '').toLowerCase());
        if (!inner) continue;
        const ph = next(`%${inner}%`);
        appendWhere(parts, params, `(LOWER(${col}::text) ILIKE ${ph} ESCAPE '\\')`);
        continue;
      }
      continue;
    }

    if (op === 'is_blank') {
      if (NUMERIC_FIELDS.has(f.field)) {
        appendWhere(parts, params, `(${col} IS NULL OR BTRIM(${col}::text) = '')`);
      } else if (DATE_FIELDS.has(f.field)) {
        appendWhere(parts, params, `(${col} IS NULL)`);
      } else if (PARTY_EQ_FIELDS.has(f.field)) {
        const partyExpr = partyFilterExpr(tableAlias, f.field);
        appendWhere(parts, params, `(${partyExpr} IS NULL OR BTRIM(${partyExpr}::text) = '')`);
      } else {
        appendWhere(parts, params, `(${col} IS NULL OR BTRIM(${col}::text) = '')`);
      }
      continue;
    }
    if (op === 'is_not_blank') {
      if (NUMERIC_FIELDS.has(f.field)) {
        appendWhere(parts, params, `(${col} IS NOT NULL AND BTRIM(${col}::text) <> '')`);
      } else if (DATE_FIELDS.has(f.field)) {
        appendWhere(parts, params, `(${col} IS NOT NULL)`);
      } else if (PARTY_EQ_FIELDS.has(f.field)) {
        const partyExpr = partyFilterExpr(tableAlias, f.field);
        appendWhere(parts, params, `(${partyExpr} IS NOT NULL AND BTRIM(${partyExpr}::text) <> '')`);
      } else {
        appendWhere(parts, params, `(${col} IS NOT NULL AND BTRIM(${col}::text) <> '')`);
      }
      continue;
    }

    if (op === 'in') {
      const vals = Array.isArray(f.values) ? f.values : [];
      if (NUMERIC_FIELDS.has(f.field)) {
        const nums = vals.map((v) => parseFactNumeric(v, f.field)).filter((n) => n != null);
        if (!nums.length) continue;
        const ph = next(nums);
        const numExpr = numericCoerceExpr(tableAlias, f.field);
        appendWhere(parts, params, `(${numExpr} = ANY(${ph}::numeric[]))`);
        continue;
      }
      if (DATE_FIELDS.has(f.field)) {
        const cleaned = vals.map((v) => String(v ?? '').trim()).filter(Boolean);
        if (!cleaned.length) continue;
        const ph = next(cleaned);
        appendWhere(parts, params, `((${col})::date = ANY(${ph}::date[]))`);
        continue;
      }
      const normVals = vals.map((v) => String(v ?? '').trim()).filter(Boolean);
      if (!normVals.length) continue;
      const ph = next(normVals);
      if (PARTY_EQ_FIELDS.has(f.field)) {
        const partyExpr = partyFilterExpr(tableAlias, f.field);
        appendWhere(parts, params, `(${partyExpr} = ANY(${ph}::text[]))`);
      } else {
        appendWhere(parts, params, `(BTRIM(COALESCE(${col}::text, '')) = ANY(${ph}::text[]))`);
      }
      continue;
    }

    if (op === 'contains') {
      const inner = escapeIlike(String(f.value ?? '').toLowerCase());
      if (!inner) continue;
      const ph = next(`%${inner}%`);
      appendWhere(parts, params, `(${col}::text ILIKE ${ph} ESCAPE '\\')`);
      continue;
    }

    if (NUMERIC_FIELDS.has(f.field) && ['gt', 'gte', 'lt', 'lte'].includes(op)) {
      const c = parseFactNumeric(f.value, f.field);
      if (c == null) continue;
      const ph = next(c);
      const map = { gt: '>', gte: '>=', lt: '<', lte: '<=' };
      const numExpr = numericCoerceExpr(tableAlias, f.field);
      appendWhere(parts, params, `(${numExpr} ${map[op]} ${ph})`);
      continue;
    }

    if (NUMERIC_FIELDS.has(f.field) && op === 'eq') {
      const raw = String(f.value ?? '').trim();
      const numExpr = numericCoerceExpr(tableAlias, f.field);
      if (raw === '') {
        appendWhere(parts, params, `(${numExpr} IS NULL)`);
        continue;
      }
      const n = parseFactNumeric(f.value, f.field);
      if (n == null) continue;
      const ph = next(n);
      appendWhere(parts, params, `(${numExpr} = ${ph})`);
      continue;
    }

    if (DATE_FIELDS.has(f.field) && op === 'eq') {
      const v = String(f.value ?? '').trim();
      if (!v) {
        appendWhere(parts, params, `(${col} IS NULL)`);
        continue;
      }
      const ph = next(v);
      appendWhere(parts, params, `(((${col})::date) = (${ph})::date)`);
      continue;
    }

    if (op === 'eq') {
      const v = String(f.value ?? '').trim();
      if (v === '') {
        if (PARTY_EQ_FIELDS.has(f.field)) {
          const partyExpr = partyFilterExpr(tableAlias, f.field);
          appendWhere(parts, params, `(${partyExpr} IS NULL OR BTRIM(${partyExpr}::text) = '')`);
        } else {
          appendWhere(parts, params, `(${col} IS NULL OR BTRIM(${col}::text) = '')`);
        }
        continue;
      }
      const ph = next(v);
      if (PARTY_EQ_FIELDS.has(f.field)) {
        const partyExpr = partyFilterExpr(tableAlias, f.field);
        appendWhere(parts, params, `(${partyExpr} = ${ph})`);
      } else {
        appendWhere(parts, params, `(BTRIM(COALESCE(${col}::text, '')) = BTRIM(${ph}::text))`);
      }
      continue;
    }
  }

  const whereSql = parts.length ? `WHERE ${parts.join(' AND ')}` : '';
  return { whereSql, params };
}

const SALES_PIVOT_MV_NAME = process.env.PIVOT_SOURCE_RELATION || 'sales_pivot_mv';
const MV_SUM_FIELD_MAP = {
  sl_qty: 'sum_sl_qty',
  net_amount: 'sum_net_amount',
};

function matchSalesPivotMvEligible(normalized) {
  const vals = normalized.values || [];
  if (!vals.length) return false;
  for (const v of vals) {
    const agg = String(v?.agg || '').toLowerCase();
    if (!['sum', 'count'].includes(agg)) return false;
    if (agg === 'sum' && !MV_SUM_FIELD_MAP[v.field]) return false;
  }
  return true;
}

async function queryPivotFromSalesPivotMv(normalized, sqlFilters) {
  const mvRel = quoteMvName(SALES_PIVOT_MV_NAME);
  const { rows: rowFields, columns: colFields, values } = normalized;
  const selectDim = [];
  const groupDim = [];
  let idx = 0;
  for (const f of rowFields) {
    const a = `dr_${idx++}`;
    const temporalExpr = derivedTemporalAxisExpr(f, 'mv');
    const col = `mv.${quoteIdent(f)}`;
    selectDim.push(`${temporalExpr || col} AS ${a}`);
    groupDim.push(temporalExpr || col);
  }
  let c = 0;
  for (const f of colFields) {
    const a = `dc_${c++}`;
    const temporalExpr = derivedTemporalAxisExpr(f, 'mv');
    const col = `mv.${quoteIdent(f)}`;
    selectDim.push(`${temporalExpr || col} AS ${a}`);
    groupDim.push(temporalExpr || col);
  }

  const aggSelects = [];
  values.forEach((v, vi) => {
    const agg = String(v.agg || '').toLowerCase();
    if (agg === 'count') {
      aggSelects.push(`SUM(COALESCE(mv.fact_row_count, 1))::bigint AS agg_cnt_${vi}`);
      return;
    }
    const mvSumCol = `mv.${quoteIdent(MV_SUM_FIELD_MAP[v.field])}`;
    aggSelects.push(`SUM(${mvSumCol}) AS agg_sum_${vi}`);
    aggSelects.push(`SUM(COALESCE(mv.fact_row_count, 1))::bigint AS agg_rowcnt_${vi}`);
  });

  const { whereSql, params } = buildWhereFromSqlFilters(sqlFilters, 'mv');
  const groupClause = groupDim.length ? `GROUP BY ${groupDim.map((_, i) => i + 1).join(', ')}` : '';
  const sql = `
    SELECT ${[...selectDim, ...aggSelects].join(',\n      ')}
    FROM ${mvRel} mv
    ${whereSql}
    ${groupClause}
  `.trim();
  return withPivotSqlClient(async (client) => {
    const groupRes = await client.query(sql, params);
    const filteredRowCount = filteredRowCountFromGroupRows(groupRes.rows, values);
    return { rows: groupRes.rows, filteredRowCount };
  });
}

async function queryPivotFromAllDimsMv(normalized, sqlFilters) {
  const mvRel = quoteMvName(SALES_ALL_DIMS_MV_NAME);
  const { rows: rowFields, columns: colFields, values } = normalized;
  const selectDim = [];
  const groupDim = [];
  let idx = 0;
  for (const f of rowFields) {
    const a = `dr_${idx++}`;
    selectDim.push(`mv.${quoteIdent(f)} AS ${a}`);
    groupDim.push(`mv.${quoteIdent(f)}`);
  }
  let c = 0;
  for (const f of colFields) {
    const a = `dc_${c++}`;
    selectDim.push(`mv.${quoteIdent(f)} AS ${a}`);
    groupDim.push(`mv.${quoteIdent(f)}`);
  }

  const aggSelects = [];
  values.forEach((v, vi) => {
    const agg = String(v.agg || '').toLowerCase();
    if (agg === 'count') {
      aggSelects.push(`SUM(COALESCE(mv.row_count, 0))::bigint AS agg_cnt_${vi}`);
      return;
    }
    const field = String(v.field || '').toLowerCase();
    if (field === 'net_amount') {
      aggSelects.push(`SUM(COALESCE(mv.sum_net_amount, 0)) AS agg_sum_${vi}`);
      aggSelects.push(`SUM(COALESCE(mv.row_count, 0))::bigint AS agg_rowcnt_${vi}`);
      return;
    }
    aggSelects.push(`SUM(COALESCE(mv.sum_sl_qty, 0)) AS agg_sum_${vi}`);
    aggSelects.push(`SUM(COALESCE(mv.row_count, 0))::bigint AS agg_rowcnt_${vi}`);
  });

  const { whereSql, params } = buildWhereFromSqlFilters(sqlFilters, 'mv');
  const groupClause = groupDim.length ? `GROUP BY ${groupDim.map((_, i) => i + 1).join(', ')}` : '';
  const sql = `
    SELECT ${[...selectDim, ...aggSelects].join(',\n      ')}
    FROM ${mvRel} mv
    ${whereSql}
    ${groupClause}
  `.trim();
  return withPivotSqlClient(async (client) => {
    const groupRes = await client.query(sql, params);
    const filteredRowCount = filteredRowCountFromGroupRows(groupRes.rows, values);
    return { rows: groupRes.rows, filteredRowCount };
  });
}

/** GROUP BY expression: BTRIM text dims (matches Excel import trim) so e.g. "DON AND JULIO " does not split from "DON AND JULIO". */
function dimSelectExpr(field, alias) {
  const col = `sd.${quoteIdent(field)}`;
  const derivedTemporal = derivedTemporalAxisExpr(field, 'sd');
  if (derivedTemporal) {
    return `${derivedTemporal} AS ${alias}`;
  }
  if (DATE_FIELDS.has(field)) {
    return `(${col})::date AS ${alias}`;
  }
  return `(
    CASE
      WHEN ${col} IS NULL OR BTRIM(${col}::text) = '' THEN NULL
      ELSE BTRIM(${col}::text)
    END
  ) AS ${alias}`;
}

function dimGroupExpr(field) {
  const col = `sd.${quoteIdent(field)}`;
  const derivedTemporal = derivedTemporalAxisExpr(field, 'sd');
  if (derivedTemporal) {
    return derivedTemporal;
  }
  if (DATE_FIELDS.has(field)) {
    return `(${col})::date`;
  }
  return `(
    CASE
      WHEN ${col} IS NULL OR BTRIM(${col}::text) = '' THEN NULL
      ELSE BTRIM(${col}::text)
    END
  )`;
}

/** Fact numeric for SUM/AVG/MIN/MAX (same coercion as WHERE on measures). */
function aggNumericExpr(field) {
  return numericCoerceExpr('sd', field);
}

function aggSelectSql(valueSpec, idx) {
  const { field, agg } = valueSpec;
  const num = aggNumericExpr(field);
  const a = String(agg || '').toLowerCase();
  if (a === 'count') {
    return {
      select: `COUNT(*)::bigint AS agg_cnt_${idx}`,
      // count metric uses row count per group (same as Node addValue)
    };
  }
  if (a === 'sum') {
    return {
      select: `SUM(${num}) AS agg_sum_${idx}, COUNT(*)::bigint AS agg_rowcnt_${idx}`,
    };
  }
  if (a === 'avg') {
    return {
      select: `SUM(${num}) AS agg_avgsum_${idx}, COUNT(*)::bigint AS agg_rowcnt_${idx}`,
    };
  }
  if (a === 'min') {
    return {
      select: `MIN(${num}) AS agg_min_${idx}, COUNT(*)::bigint AS agg_rowcnt_${idx}`,
    };
  }
  if (a === 'max') {
    return {
      select: `MAX(${num}) AS agg_max_${idx}, COUNT(*)::bigint AS agg_rowcnt_${idx}`,
    };
  }
  throw new Error(`Unsupported agg: ${agg}`);
}

/**
 * Read pre-aggregated brand × state × month (sum net_amount, row counts).
 */
async function queryPivotFromBrandStateMonthMv(sqlFilters) {
  const mvName = process.env.PIVOT_MV_BRAND_STATE_MONTH || 'mv_sales_brand_state_month';
  const mvRel = quoteMvName(mvName);
  const { whereSql: wMv, params: pMv } = buildWhereFromSqlFilters(sqlFilters, 'mv');
  const { whereSql: wSd, params: pSd } = buildWhereFromSqlFilters(sqlFilters, 'sd');

  const groupSql = `
    SELECT
      mv.brand AS dr_0,
      mv.state AS dr_1,
      mv.month AS dr_2,
      mv.sum_net_amount AS agg_sum_0,
      mv.fact_row_count AS agg_rowcnt_0
    FROM ${mvRel} mv
    ${wMv}
  `.trim();

  const countSql = `
    SELECT COUNT(*)::bigint AS c
    FROM sales_data sd
    ${wSd}
  `.trim();

  return withPivotSqlClient(async (client) => {
    // Second query is COUNT(*) over filtered sales_data (~1M+ rows). Default off; exact total: PIVOT_PG_SKIP_FACT_ROW_COUNT=0
    const skipFactCount = String(process.env.PIVOT_PG_SKIP_FACT_ROW_COUNT || '1').trim() !== '0';
    const groupRes = await client.query(groupSql, pMv);
    let filteredRowCount;
    if (skipFactCount) {
      filteredRowCount = groupRes.rows.reduce((s, r) => s + (Number(r.fact_row_count) || 0), 0);
    } else {
      const countRes = await client.query(countSql, pSd);
      filteredRowCount = Number(countRes.rows[0]?.c ?? 0);
    }
    return { rows: groupRes.rows, filteredRowCount };
  });
}

/**
 * @returns {{ rows: Record<string, unknown>[], filteredRowCount: number }}
 */
export async function queryPivotGroupBy(normalized, sqlFilters) {
  const pool = getPivotSqlPool();
  if (!pool) throw new Error('No database pool');

  return queryPivotFromAllDimsMv(normalized, sqlFilters);
}

function splitPivotKey(key, n) {
  if (n <= 0) return [];
  const s = String(key ?? '');
  if (n === 1) return [s];
  const parts = s.split('||');
  return parts.length === n ? parts : null;
}

export async function queryDrilldownPage(normalized, sqlFilters, drill) {
  const pool = getPivotSqlPool();
  if (!pool) throw new Error('No database pool');

  const { rows: rowFields, columns: colFields } = normalized;
  const offset = Math.max(0, Number(drill.offset) || 0);
  const limit = Math.max(1, Math.min(1000, Number(drill.limit) || 200));

  const { whereSql, params } = buildWhereFromSqlFilters(sqlFilters);
  const parts = whereSql ? [whereSql.replace(/^WHERE\s+/i, '')] : [];
  const p = [...params];
  let i = p.length + 1;

  const addParam = (v) => {
    p.push(v);
    return `$${i++}`;
  };

  const pushDrillParts = (fields, key) => {
    if (fields.length === 0 || !key || key === '(all)') return;
    const labels = splitPivotKey(key, fields.length);
    if (!labels) throw new Error('Invalid drill key');
    fields.forEach((field, j) => {
      const col = `sd.${quoteIdent(field)}`;
      const label = labels[j];
      if (label === '(blank)') {
        parts.push(`(${col} IS NULL OR BTRIM(${col}::text) = '')`);
      } else if (DATE_FIELDS.has(field)) {
        parts.push(`(((${col})::date) = ${addParam(label)}::date)`);
      } else {
        parts.push(`(BTRIM(COALESCE(${col}::text, '')) = BTRIM(${addParam(label)}::text))`);
      }
    });
  };

  pushDrillParts(rowFields, drill.rowKey);
  pushDrillParts(colFields, drill.columnKey);

  const whereCombined = parts.length ? `WHERE ${parts.join(' AND ')}` : '';

  const countSql = `SELECT COUNT(*)::bigint AS c FROM sales_data sd ${whereCombined}`;
  const dataSql = `
    SELECT sd.*
    FROM sales_data sd
    ${whereCombined}
    ORDER BY sd.id ASC
    LIMIT ${limit} OFFSET ${offset}
  `.trim();

  return withPivotSqlClient(async (client) => {
    const countRes = await client.query(countSql, p);
    const dataRes = await client.query(dataSql, p);
    return {
      total: Number(countRes.rows[0]?.c ?? 0),
      rows: dataRes.rows,
    };
  }, { timeoutMs: getPivotSupportingSqlTimeoutMs() });
}
