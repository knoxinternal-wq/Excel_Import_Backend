import { supabase } from '../models/supabase.js';
import { MAX_SALES_ROWS } from '../config/constants.js';
import { SALES_DATA_NUMERIC_COLUMNS_SET, parseFactNumeric } from '../utils/salesFacts.js';
import { normalizeAgentName, getAgentNameExactKey } from '../utils/normalizeHeader.js';
import { getAgentNameMasterMap } from './masterLoaders.js';
import {
  getPivotSqlAggregationDetails,
  isPivotSqlDrilldownEligible,
  getPivotSqlPool,
  queryDistinctPivotFilterValues,
  queryPivotGroupBy,
  queryDrilldownPage,
} from './pivotSql.js';
import PQueue from 'p-queue';
import { logDebug, logInfo, logWarn } from '../utils/logger.js';
import {
  isPivotRedisConfigured,
  pivotRedisGet,
  pivotRedisSet,
} from './pivotRedisCache.js';
import { getPivotCapabilities as buildPivotCapabilities } from './pivotMvResolver.js';

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

const NUMERIC_FIELDS = SALES_DATA_NUMERIC_COLUMNS_SET;

const DATE_FIELDS = new Set(['bill_date', 'sale_order_date', 'created_at']);

/** Row/column dims that explode pivot size — surfaced in meta.warnings. */
const HIGH_CARDINALITY_PIVOT_FIELDS = new Set([
  'to_party_name',
  'party_name_for_count',
  'party_grouped',
  'bill_no',
  'item_no',
  'sale_order_no',
]);

function getPivotCardinalityWarnings(rows, columns) {
  const warnings = [];
  const axes = [...(rows || []), ...(columns || [])];
  for (const f of axes) {
    if (HIGH_CARDINALITY_PIVOT_FIELDS.has(f)) {
      warnings.push(
        `High-cardinality dimension "${f}" can make the pivot very slow. Prefer filters, state/branch/brand, or grouped fields.`,
      );
    }
  }
  return warnings;
}

function assertPivotCardinalityPreflight(normalized) {
  if (String(process.env.PIVOT_PREFLIGHT_BLOCK_WIDE || '1').trim() === '0') return;
  const axes = [...(normalized.rows || []), ...(normalized.columns || [])];
  if (!axes.length) return;
  const hasAnyFilter = Array.isArray(normalized.filters) && normalized.filters.length > 0;
  if (hasAnyFilter) return;
  const highCount = axes.filter((f) => HIGH_CARDINALITY_PIVOT_FIELDS.has(f)).length;
  const hasExplosiveField = axes.includes('item_no') || axes.includes('bill_no');
  // Only hard-block very high-risk unfiltered layouts that are known to explode cardinality.
  const clearlyExplosive = hasExplosiveField && axes.length >= 3;
  const tooManyRawHighCardAxes = highCount >= 3 && axes.length >= 4;
  if (clearlyExplosive || tooManyRawHighCardAxes) {
    throw new Error(
      'Pivot layout is too wide for unfiltered raw fields. Add at least one filter or reduce high-cardinality row/column fields (item_no, bill_no, party fields).',
    );
  }
}

/** When no Postgres pool, these stay in-memory; with `DATABASE_URL` they map to SQL + PostgREST. */
const SQL_DEFER_OPS = new Set(['is_blank', 'is_not_blank']);

/** ilike(any) OR-clause size; larger lists stay in-memory to avoid huge URLs. */
const MAX_SQL_STRING_IN = 40;

const PIVOT_PAGE_SIZE = 1000;
const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

function isGrandTotalRow(row) {
  const branch = row?.branch != null ? String(row.branch).trim().toLowerCase() : '';
  return branch === 'total' || branch === 'grand total' || branch.includes('grand total') || branch.includes('grandtotal');
}

/** In-process pivot payload cache; default 180s (large tables + repeat layouts). */
const PIVOT_RESULT_CACHE_TTL_MS = Number(process.env.PIVOT_MEMORY_CACHE_TTL_MS) || 180_000;
const PIVOT_RESULT_CACHE_MAX = 40;
const pivotResultCache = new Map();
const pivotResultInflight = new Map();
const MAX_PIVOT_VISIBLE_CELLS = Math.max(
  50_000,
  Number(process.env.PIVOT_MAX_VISIBLE_CELLS) || 250_000,
);

function isWithinVisibleCellLimit(rowCount, colCount, metricCount) {
  const safeRowCount = Math.max(1, Number(rowCount) || 1);
  const safeColCount = Math.max(1, Number(colCount) || 1);
  const safeMetricCount = Math.max(1, Number(metricCount) || 1);
  const visibleCells = safeRowCount * safeColCount * safeMetricCount;
  return visibleCells <= MAX_PIVOT_VISIBLE_CELLS;
}

function pivotResultCacheKey(normalized) {
  return JSON.stringify({
    rows: normalized.rows,
    columns: normalized.columns,
    values: normalized.values,
    filters: normalized.filters,
    sort: normalized.sort,
    limitRows: normalized.limitRows,
    subtotalFields: normalized.subtotalFields,
  });
}

/**
 * Escape ILIKE special chars (backslash, %, _) for use inside ILIKE patterns.
 */
function escapeIlikePattern(s) {
  return String(s ?? '')
    .replace(/\\/g, '\\\\')
    .replace(/%/g, '\\%')
    .replace(/_/g, '\\_');
}

function enrichAgentNameFields(rows, agentNameMap) {
  return (rows || []).map((row) => {
    const existingFinal = row?.agent_name_final != null ? String(row.agent_name_final).trim() : '';
    if (existingFinal) {
      const existingCorrection = row?.agent_names_correction != null ? String(row.agent_names_correction).trim() : '';
      const display = existingCorrection || existingFinal;
      return {
        ...row,
        agent_names_correction: display,
        agent_name_final: existingFinal,
      };
    }
    const rawAgentName = row?.agent_name != null ? String(row.agent_name).trim() : '';
    const excelName = rawAgentName || null;
    const existingCorrection = row?.agent_names_correction != null ? String(row.agent_names_correction).trim() : '';
    const exactKey = getAgentNameExactKey(row?.agent_name);
    const normalizedKey = normalizeAgentName(row?.agent_name);
    const combinedName = (exactKey || normalizedKey)
      ? (agentNameMap?.get(exactKey) ?? agentNameMap?.get(normalizedKey) ?? null)
      : null;
    const mapped = combinedName || excelName || existingCorrection || null;
    return {
      ...row,
      agent_names_correction: mapped,
      agent_name_final: mapped,
    };
  });
}

function derivePivotTemporalFields(row) {
  const billDate = row?.bill_date;
  let d = null;
  if (billDate) {
    if (billDate instanceof Date) {
      d = billDate;
    } else {
      const s = String(billDate).trim();
      const ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
      if (ymd) d = new Date(parseInt(ymd[1], 10), parseInt(ymd[2], 10) - 1, parseInt(ymd[3], 10));
      else d = new Date(s);
    }
  }
  if (d && !Number.isNaN(d.getTime()) && d.getFullYear() >= 2000) {
    const year = d.getFullYear();
    const monthNum = d.getMonth() + 1;
    const fyYear = monthNum >= 4 ? year : year - 1;
    const mmmLabel = MONTH_NAMES[monthNum - 1] || '';
    return {
      fy: `${fyYear}-${String(fyYear + 1).slice(-2)}`,
      month: mmmLabel ? `${mmmLabel}-${String(year).slice(-2)}` : null,
      mmm: mmmLabel ? mmmLabel.toUpperCase() : null,
    };
  }
  const monthParsed = parsePivotMmmYy(row?.month);
  if (monthParsed) {
    const year = monthParsed.y;
    const monthNum = monthParsed.m + 1;
    const fyYear = monthNum >= 4 ? year : year - 1;
    const mmmLabel = MONTH_NAMES[monthParsed.m] || '';
    return {
      fy: `${fyYear}-${String(fyYear + 1).slice(-2)}`,
      month: mmmLabel ? `${mmmLabel}-${String(year).slice(-2)}` : null,
      mmm: mmmLabel ? mmmLabel.toUpperCase() : null,
    };
  }
  return { fy: null, month: null, mmm: null };
}

/**
 * Align agent display with stored values when present (import always sets them).
 * Legacy rows with empty final/correction still resolve via agent_name_master once per process.
 */
async function enrichPage(rows, agentMapHolder) {
  const page = rows || [];
  if (page.length === 0) return page;
  const allHaveFinal = page.every((r) => String(r?.agent_name_final ?? '').trim());
  if (allHaveFinal) {
    return page.map((row) => {
      const ec = row?.agent_names_correction != null ? String(row.agent_names_correction).trim() : '';
      const ef = String(row.agent_name_final ?? '').trim();
      const display = ec || ef;
      return { ...row, agent_names_correction: display, agent_name_final: ef };
    });
  }
  if (!agentMapHolder.map) agentMapHolder.map = await getAgentNameMasterMap();
  return enrichAgentNameFields(page, agentMapHolder.map);
}

function unique(values) {
  return [...new Set(values.filter(Boolean))];
}

/** Excel-style row/column label order; supports legacy { axis, direction }. */
function normalizePivotSort(sort) {
  if (!sort || typeof sort !== 'object') {
    return { rows: 'asc', columns: 'asc' };
  }
  if ('rows' in sort || 'columns' in sort) {
    return {
      rows: sort.rows === 'desc' ? 'desc' : 'asc',
      columns: sort.columns === 'desc' ? 'desc' : 'asc',
    };
  }
  const dir = sort.direction === 'desc' ? 'desc' : 'asc';
  if (sort.axis === 'columns') return { rows: 'asc', columns: dir };
  return { rows: dir, columns: 'asc' };
}

function sortPivotHeaderList(headers, fields, direction) {
  const sorted = [...headers].sort((a, b) => comparePivotHeaders(a, b, fields));
  if (direction === 'desc') sorted.reverse();
  return sorted;
}

function normalizeFiltersInput(rawFilters) {
  if (Array.isArray(rawFilters)) return rawFilters;
  if (!rawFilters || typeof rawFilters !== 'object') return [];
  const out = [];
  for (const [field, spec] of Object.entries(rawFilters)) {
    if (!field || !spec || typeof spec !== 'object') continue;
    out.push({
      field,
      operator: spec.operator ?? 'eq',
      value: spec.value,
      values: spec.values,
    });
  }
  return out;
}

/** When rows/columns are set but Values is empty — same idea as Excel (implicit row count per cell). */
const DEFAULT_PIVOT_VALUES_WHEN_AXES_ONLY = Object.freeze([
  { field: 'id', agg: 'count', label: 'Count of rows' },
]);

function normalizeSubtotalFields(rawSubtotalFields, rows) {
  const allowed = new Set(rows || []);
  return unique((Array.isArray(rawSubtotalFields) ? rawSubtotalFields : [])
    .map((f) => String(f || '').trim())
    .filter((f) => allowed.has(f)));
}

function normalizeConfig(config = {}) {
  const rows = unique((config.rows || []).filter((f) => SALES_FIELDS.includes(f)));
  const columns = unique((config.columns || []).filter((f) => SALES_FIELDS.includes(f)));
  let values = (config.values || [])
    .filter((v) => v && SALES_FIELDS.includes(v.field))
    .map((v) => ({
      field: v.field,
      agg: String(v.agg || 'sum').toLowerCase(),
      label: v.label || `${String(v.agg || 'sum').toUpperCase()} ${v.field}`,
    }))
    .filter((v) => ['sum', 'count', 'avg', 'min', 'max'].includes(v.agg));
  if (values.length === 0 && (rows.length > 0 || columns.length > 0)) {
    values = [...DEFAULT_PIVOT_VALUES_WHEN_AXES_ONLY];
  }
  const filters = normalizeFiltersInput(config.filters);
  const sort = normalizePivotSort(config.sort);
  const rawLimit = Number(config.limitRows);
  const limitRows = Number.isFinite(rawLimit) && rawLimit > 0
    ? Math.min(MAX_SALES_ROWS, Math.floor(rawLimit))
    : MAX_SALES_ROWS;
  const subtotalFields = normalizeSubtotalFields(config.subtotalFields, rows);
  return { rows, columns, values, filters, sort, limitRows, subtotalFields };
}

function formatAxisValue(v, field) {
  if (v == null || v === '') return '(blank)';
  if (DATE_FIELDS.has(field)) {
    const d = new Date(v);
    if (!Number.isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  }
  const s = String(v).trim();
  return s === '' ? '(blank)' : s;
}

function axisValueFromRow(row, field) {
  const primary = row?.[field];
  if ((field === 'fy' || field === 'month' || field === 'mmm') && (primary == null || String(primary).trim() === '')) {
    const derived = derivePivotTemporalFields(row);
    return formatAxisValue(derived[field], field);
  }
  if ((primary == null || String(primary).trim() === '')
      && (field === 'agent_names_correction' || field === 'agent_name_final')) {
    const fallback = row?.agent_name;
    return formatAxisValue(fallback, 'agent_name');
  }
  return formatAxisValue(primary, field);
}

function keyFromParts(parts) {
  return parts.join('||');
}

/** Month / FY aware ordering for pivot axes (avoids alphabetical month labels like Apr-25, Jan-26). */
const PIVOT_MONTH_ABBREV = {
  jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
  jul: 6, aug: 7, sep: 8, sept: 8, oct: 9, nov: 10, dec: 11,
};

const PIVOT_SORT_TYPE_PRIO = { date: 0, fy: 1, month: 2, str: 3, blank: 4 };

function isPivotBlankAxisLabel(v) {
  const s = String(v ?? '');
  return s === '' || s === '(blank)';
}

function parsePivotMmmYy(s) {
  const str = String(s ?? '').trim();
  if (!str || isPivotBlankAxisLabel(str)) return null;
  const m = str.match(/^([A-Za-z]{3,9})\s*[-/]\s*(\d{2}|\d{4})$/);
  if (!m) return null;
  const monKey = m[1].toLowerCase().slice(0, 3);
  const mo = PIVOT_MONTH_ABBREV[monKey];
  if (mo === undefined) return null;
  let y = parseInt(m[2], 10);
  if (!Number.isFinite(y)) return null;
  if (y < 100) y += 2000;
  return { y, m: mo };
}

function parsePivotMmmOnly(s) {
  const str = String(s ?? '').trim();
  if (!str || str.length < 3) return null;
  if (/[-/]\d/.test(str)) return null;
  const monKey = str.toLowerCase().slice(0, 3);
  const mo = PIVOT_MONTH_ABBREV[monKey];
  if (mo === undefined) return null;
  return { y: 0, m: mo };
}

function parsePivotFyStart(s) {
  const v = String(s ?? '').trim();
  if (!v || isPivotBlankAxisLabel(v)) return null;
  const m = v.match(/^(\d{4})\D(\d{2,4})$/);
  if (m) return parseInt(m[1], 10);
  return null;
}

function parsePivotDateMsForSort(s) {
  const str = String(s ?? '').trim();
  if (!str || isPivotBlankAxisLabel(str)) return null;
  const t = Date.parse(str);
  return Number.isNaN(t) ? null : t;
}

function pivotDimSortKey(val, field) {
  if (isPivotBlankAxisLabel(val)) return { type: 'blank' };
  const v = String(val);
  if (DATE_FIELDS.has(field)) {
    const ms = parsePivotDateMsForSort(v);
    if (ms != null) return { type: 'date', n: ms };
  }
  if (field === 'fy') {
    const y = parsePivotFyStart(v);
    if (y != null) return { type: 'fy', n: y };
  }
  if (field === 'month') {
    const my = parsePivotMmmYy(v);
    if (my) return { type: 'month', y: my.y, m: my.m };
  }
  if (field === 'mmm') {
    const my = parsePivotMmmYy(v) || parsePivotMmmOnly(v);
    if (my) return { type: 'month', y: my.y, m: my.m };
  }
  return { type: 'str', s: v };
}

function comparePivotDimSortKeys(ka, kb) {
  if (ka.type !== kb.type) return PIVOT_SORT_TYPE_PRIO[ka.type] - PIVOT_SORT_TYPE_PRIO[kb.type];
  switch (ka.type) {
    case 'blank': return 0;
    case 'date': return ka.n - kb.n;
    case 'fy': return ka.n - kb.n;
    case 'month':
      if (ka.y !== kb.y) return ka.y - kb.y;
      return ka.m - kb.m;
    case 'str':
      return ka.s.localeCompare(kb.s, undefined, { sensitivity: 'base', numeric: true });
    default:
      return 0;
  }
}

function comparePivotHeaders(a, b, fields) {
  if (!fields?.length) return String(a.key).localeCompare(String(b.key));
  const la = a.labels || [];
  const lb = b.labels || [];
  for (let i = 0; i < fields.length; i += 1) {
    const field = fields[i];
    const va = la[i];
    const vb = lb[i];
    const cmp = comparePivotDimSortKeys(pivotDimSortKey(va, field), pivotDimSortKey(vb, field));
    if (cmp !== 0) return cmp;
  }
  return String(a.key).localeCompare(String(b.key));
}

function normText(v) {
  return String(v ?? '').trim().toLowerCase();
}

function initMetricBucket() {
  return { sum: 0, count: 0, min: null, max: null };
}

function applyMetric(bucket, rawVal, field) {
  const n = parseFactNumeric(rawVal, field);
  bucket.count += 1;
  if (n == null) return;
  bucket.sum += n;
  bucket.min = bucket.min == null ? n : Math.min(bucket.min, n);
  bucket.max = bucket.max == null ? n : Math.max(bucket.max, n);
}

function readMetric(bucket, agg) {
  if (!bucket) return null;
  if (agg === 'sum') return bucket.sum;
  if (agg === 'count') return bucket.count;
  if (agg === 'avg') return bucket.count ? bucket.sum / bucket.count : null;
  if (agg === 'min') return bucket.min;
  if (agg === 'max') return bucket.max;
  return null;
}

function ensureCell(map, rowKey, colKey) {
  if (!map.has(rowKey)) map.set(rowKey, new Map());
  const row = map.get(rowKey);
  if (!row.has(colKey)) row.set(colKey, new Map());
  return row.get(colKey);
}

function addValue(cellMetricMap, metricKey, rawVal, field) {
  if (!cellMetricMap.has(metricKey)) cellMetricMap.set(metricKey, initMetricBucket());
  applyMetric(cellMetricMap.get(metricKey), rawVal, field);
}

function materializeBucketsFromSqlRow(sqlRow, values) {
  const metricMap = new Map();
  for (let vi = 0; vi < values.length; vi += 1) {
    const v = values[vi];
    const mk = `${v.agg}:${v.field}`;
    const b = initMetricBucket();
    const a = String(v.agg).toLowerCase();
    if (a === 'count') {
      b.count = Number(sqlRow[`agg_cnt_${vi}`]) || 0;
    } else if (a === 'sum') {
      const x = sqlRow[`agg_sum_${vi}`];
      b.sum = x == null || Number.isNaN(Number(x)) ? 0 : Number(x);
      b.count = Number(sqlRow[`agg_rowcnt_${vi}`]) || 0;
    } else if (a === 'avg') {
      const x = sqlRow[`agg_avgsum_${vi}`];
      b.sum = x == null || Number.isNaN(Number(x)) ? 0 : Number(x);
      b.count = Number(sqlRow[`agg_rowcnt_${vi}`]) || 0;
    } else if (a === 'min') {
      const x = sqlRow[`agg_min_${vi}`];
      b.min = x == null || x === '' || Number.isNaN(Number(x)) ? null : Number(x);
      b.count = Number(sqlRow[`agg_rowcnt_${vi}`]) || 0;
    } else if (a === 'max') {
      const x = sqlRow[`agg_max_${vi}`];
      b.max = x == null || x === '' || Number.isNaN(Number(x)) ? null : Number(x);
      b.count = Number(sqlRow[`agg_rowcnt_${vi}`]) || 0;
    }
    metricMap.set(mk, b);
  }
  return metricMap;
}

function filterMatch(row, f) {
  if (!f || !f.field || !SALES_FIELDS.includes(f.field)) return true;
  const val = row[f.field];
  const op = String(f.operator || 'eq').toLowerCase();

  if (f.field === 'id') {
    const vStr = val != null && val !== '' ? String(val).trim() : '';
    if (op === 'is_blank') return vStr === '';
    if (op === 'is_not_blank') return vStr !== '';
    if (op === 'in') {
      const vals = Array.isArray(f.values) ? f.values.map((x) => String(x ?? '').trim()).filter(Boolean) : [];
      if (!vals.length) return true;
      return vals.includes(vStr);
    }
    if (op === 'contains') return vStr.toLowerCase().includes(String(f.value ?? '').toLowerCase());
    if (op === 'eq') return vStr === String(f.value ?? '').trim();
    const n = parseFactNumeric(val, f.field);
    const c = parseFactNumeric(f.value, f.field);
    if (op === 'gt') return n != null && c != null && n > c;
    if (op === 'gte') return n != null && c != null && n >= c;
    if (op === 'lt') return n != null && c != null && n < c;
    if (op === 'lte') return n != null && c != null && n <= c;
    return true;
  }

  if (op === 'is_blank') return val == null || String(val).trim() === '';
  if (op === 'is_not_blank') return !(val == null || String(val).trim() === '');
  if (op === 'in') {
    const vals = Array.isArray(f.values) ? f.values.map((x) => normText(x)).filter(Boolean) : [];
    if (!vals.length) return true;
    return vals.includes(normText(val));
  }
  if (op === 'contains') return String(val ?? '').toLowerCase().includes(String(f.value ?? '').toLowerCase());
  if (NUMERIC_FIELDS.has(f.field)) {
    const n = parseFactNumeric(val, f.field);
    const c = parseFactNumeric(f.value, f.field);
    if (op === 'gt') return n != null && c != null && n > c;
    if (op === 'gte') return n != null && c != null && n >= c;
    if (op === 'lt') return n != null && c != null && n < c;
    if (op === 'lte') return n != null && c != null && n <= c;
  }
  if (DATE_FIELDS.has(f.field) && ['gt', 'gte', 'lt', 'lte'].includes(op)) {
    let aStr = val instanceof Date ? val.toISOString().slice(0, 10) : String(val ?? '').trim();
    const bStr = String(f.value ?? '').trim();
    if (!aStr || !bStr) return false;
    const ta = Date.parse(aStr.includes('T') ? aStr : `${aStr}T00:00:00`);
    const tb = Date.parse(bStr.includes('T') ? bStr : `${bStr}T00:00:00`);
    if (!Number.isFinite(ta) || !Number.isFinite(tb)) return false;
    if (op === 'gt') return ta > tb;
    if (op === 'gte') return ta >= tb;
    if (op === 'lt') return ta < tb;
    if (op === 'lte') return ta <= tb;
  }
  if (!NUMERIC_FIELDS.has(f.field) && !DATE_FIELDS.has(f.field) && ['gt', 'gte', 'lt', 'lte'].includes(op)) {
    const a = normText(val);
    const b = normText(f.value);
    if (a === '' || b === '') return false;
    if (op === 'gt') return a > b;
    if (op === 'gte') return a >= b;
    if (op === 'lt') return a < b;
    if (op === 'lte') return a <= b;
  }
  return normText(val) === normText(f.value);
}

function applyFilters(rows, filters) {
  if (!filters?.length) return rows;
  return rows.filter((r) => filters.every((f) => filterMatch(r, f)));
}

const ID_FILTERS_FOR_PG = new Set(['eq', 'in', 'contains']);

/**
 * Supabase/PostgREST coerces `id` filters to the column type (UUID → rejects "0").
 * Stream pivot applies `id` in memory only; Postgres pivot adds text-safe `id` here.
 */
function pivotPostgresWhereFilters(normalized, sqlFilters) {
  const idExtras = (normalized.filters || []).filter(
    (f) => f && f.field === 'id' && ID_FILTERS_FOR_PG.has(String(f.operator || 'eq').toLowerCase()),
  );
  return [...sqlFilters, ...idExtras];
}

/**
 * Split pivot filters: SQL-applied vs in-memory (enrichment / blank / oversized IN).
 */
function splitFilters(filters) {
  const sqlFilters = [];
  const memFilters = [];
  const pool = getPivotSqlPool();
  for (const f of filters || []) {
    if (!f || !f.field || !SALES_FIELDS.includes(f.field)) continue;
    if (f.field === 'id') {
      // Keep id filters in SQL path so we don't fall back to slow in-memory streaming.
      // Postgres pivot already has special id handling in `pivotSql.buildWhereFromSqlFilters()`.
      sqlFilters.push(f);
      continue;
    }
    const op = String(f.operator || 'eq').toLowerCase();
    if (SQL_DEFER_OPS.has(op)) {
      if (pool) sqlFilters.push(f);
      else memFilters.push(f);
      continue;
    }
    if (op === 'eq' && normText(f.value) === '' && !NUMERIC_FIELDS.has(f.field) && !DATE_FIELDS.has(f.field)) {
      if (pool) sqlFilters.push(f);
      else memFilters.push(f);
      continue;
    }
    if (op === 'in') {
      const vals = Array.isArray(f.values) ? f.values : [];
      const normVals = vals.map(normText).filter(Boolean);
      if (!normVals.length) continue;
      // Large / comma-containing string IN lists are fine for Postgres (`= ANY($n::text[])`).
      // Only defer to in-memory when we must use the Supabase stream (no SQL pool).
      if (!NUMERIC_FIELDS.has(f.field) && !DATE_FIELDS.has(f.field) && !getPivotSqlPool()) {
        if (normVals.length > MAX_SQL_STRING_IN || normVals.some((v) => String(v).includes(','))) {
          memFilters.push(f);
          continue;
        }
      }
    }
    if (op === 'contains' && !String(f.value ?? '').trim()) continue;
    if (['gt', 'gte', 'lt', 'lte'].includes(op)) {
      if (NUMERIC_FIELDS.has(f.field)) {
        sqlFilters.push(f);
        continue;
      }
      if (DATE_FIELDS.has(f.field)) {
        if (pool) sqlFilters.push(f);
        else memFilters.push(f);
        continue;
      }
      if (pool) sqlFilters.push(f);
      else memFilters.push(f);
      continue;
    }
    // `%` and `_` are treated as literal characters in SQL because we escape them in `applyOneSqlFilter()`.
    // That means we can keep these filters in the SQL path instead of forcing the slow stream+in-memory fallback.
    sqlFilters.push(f);
  }
  return { sqlFilters, memFilters };
}

function applyOneSqlFilter(query, f) {
  let q = query;
  const field = f.field;
  const op = String(f.operator || 'eq').toLowerCase();

  if (op === 'is_blank') {
    if (NUMERIC_FIELDS.has(field)) {
      return q.or(`${field}.is.null,${field}.eq.`);
    }
    if (DATE_FIELDS.has(field)) {
      return q.is(field, null);
    }
    return q.or(`${field}.is.null,${field}.eq.`);
  }
  if (op === 'is_not_blank') {
    if (field === 'id') {
      return q.not('id', 'is', null).neq('id', '');
    }
    if (NUMERIC_FIELDS.has(field)) {
      return q.not(field, 'is', null).neq(field, '');
    }
    if (DATE_FIELDS.has(field)) {
      return q.not(field, 'is', null);
    }
    return q.not(field, 'is', null).neq(field, '');
  }

  if (op === 'in') {
    const vals = Array.isArray(f.values) ? f.values : [];
    if (NUMERIC_FIELDS.has(field)) {
      const nums = vals.map((v) => parseFactNumeric(v, field)).filter((n) => n != null);
      if (!nums.length) return q;
      return q.in(field, nums);
    }
    if (DATE_FIELDS.has(field)) {
      const cleaned = vals.map((v) => String(v ?? '').trim()).filter(Boolean);
      if (!cleaned.length) return q;
      return q.in(field, cleaned);
    }
    const normVals = vals.map(normText).filter(Boolean);
    if (!normVals.length) return q;
    const patterns = normVals.map((v) => escapeIlikePattern(v));
    return q.ilikeAnyOf(field, patterns);
  }

  if (op === 'contains') {
    const inner = escapeIlikePattern(String(f.value ?? '').toLowerCase());
    if (!inner) return q;
    return q.ilike(field, `%${inner}%`);
  }

  if (NUMERIC_FIELDS.has(field) && ['gt', 'gte', 'lt', 'lte'].includes(op)) {
    const c = parseFactNumeric(f.value, field);
    if (c == null) return q;
    if (op === 'gt') return q.gt(field, c);
    if (op === 'gte') return q.gte(field, c);
    if (op === 'lt') return q.lt(field, c);
    if (op === 'lte') return q.lte(field, c);
  }

  if (NUMERIC_FIELDS.has(field) && op === 'eq') {
    const raw = String(f.value ?? '').trim();
    if (raw === '') {
      return q.or(`${field}.is.null,${field}.eq.`);
    }
    const n = parseFactNumeric(f.value, field);
    if (n == null) return q;
    return q.eq(field, n);
  }

  if (DATE_FIELDS.has(field) && op === 'eq') {
    const v = String(f.value ?? '').trim();
    if (!v) return q.is(field, null);
    return q.eq(field, v);
  }

  if (op === 'eq') {
    const v = normText(f.value);
    if (v === '') {
      return q.or(`${field}.is.null,${field}.eq.`);
    }
    return q.ilike(field, escapeIlikePattern(v));
  }

  return q;
}

function applySqlFiltersToQuery(query, sqlFilters) {
  let q = query;
  for (const f of sqlFilters) {
    q = applyOneSqlFilter(q, f);
  }
  return q;
}

function pivotSelectColumns(normalized) {
  const needAgentName = [...normalized.rows, ...normalized.columns].some(
    (f) => f === 'agent_names_correction' || f === 'agent_name_final',
  );
  const needBillDateForTemporalFallback = [...normalized.rows, ...normalized.columns, ...normalized.filters.map((f) => f.field)]
    .some((f) => f === 'fy' || f === 'month' || f === 'mmm');
  const selectFields = unique([
    ...normalized.rows,
    ...normalized.columns,
    ...normalized.values.map((v) => v.field),
    ...normalized.filters.map((f) => f.field).filter(Boolean),
    ...(needAgentName ? ['agent_name'] : []),
    ...(needBillDateForTemporalFallback ? ['bill_date'] : []),
  ]);
  return unique(['id', ...selectFields]).join(',');
}

/**
 * Cursor stream over sales_data: WHERE (SQL filters) ORDER BY id.
 * Caps scanned rows at MAX_SALES_ROWS (safety).
 */
async function* streamFilteredSalesPages(selectClause, sqlFilters) {
  let lastId = 0;
  let scanned = 0;
  for (;;) {
    let q = supabase.from('sales_data').select(selectClause);
    q = applySqlFiltersToQuery(q, sqlFilters);
    const { data, error } = await q
      .gt('id', lastId)
      .order('id', { ascending: true })
      .limit(PIVOT_PAGE_SIZE);
    if (error) throw new Error(error.message);
    const page = data || [];
    if (!page.length) break;
    scanned += page.length;
    if (scanned > MAX_SALES_ROWS) {
      throw new Error(
        `Pivot scan exceeded ${MAX_SALES_ROWS.toLocaleString()} rows after filters; narrow filters.`,
      );
    }
    let maxSeen = lastId;
    for (const r of page) {
      const idNum = Number(r?.id);
      if (Number.isFinite(idNum) && idNum > maxSeen) maxSeen = idNum;
    }
    lastId = maxSeen;
    yield page.filter((row) => !isGrandTotalRow(row));
    if (page.length < PIVOT_PAGE_SIZE) break;
  }
}

/** One label per row-axis column; matches Excel-style subtotal rows (avoids length > row field count). */
function buildSubtotalDisplayLabels(prefixLabels, depth, rowAxisCols) {
  const n = Math.max(1, rowAxisCols);
  const labels = Array(n).fill('');
  if (n === 1) {
    labels[0] = 'Subtotal';
    return labels;
  }
  if (depth < n) {
    for (let i = 0; i < depth; i += 1) labels[i] = String(prefixLabels[i] ?? '');
    labels[depth] = 'Subtotal';
    return labels;
  }
  for (let i = 0; i < n - 1; i += 1) labels[i] = String(prefixLabels[i] ?? '');
  labels[n - 1] = 'Subtotal';
  return labels;
}

function buildSubtotals(rowHeaders, colHeaders, cellMap, valueKeys, rowFieldCount = 0, selectedDepths = null) {
  const allowedDepths = Array.isArray(selectedDepths)
    ? new Set(selectedDepths.filter((d) => Number.isInteger(d) && d > 0))
    : null;
  if (allowedDepths && allowedDepths.size === 0) return [];
  const byDepth = new Map();
  for (const rowHeader of rowHeaders) {
    const labLen = rowHeader.labels?.length || 0;
    const n = Math.max(1, rowFieldCount || 0, labLen);
    // Cap depth at actual labels on this row (always matches slice(0, depth)).
    const maxDepth = labLen ? Math.min(n, labLen) : 0;
    // Include depth === maxDepth when inner row field so "Subtotal" on last field works.
    for (let depth = 1; depth <= maxDepth; depth += 1) {
      if (allowedDepths && !allowedDepths.has(depth)) continue;
      const prefixLabels = rowHeader.labels.slice(0, depth);
      const subtotalKey = keyFromParts([...prefixLabels, '__subtotal__']);
      if (!byDepth.has(depth)) byDepth.set(depth, new Map());
      if (!byDepth.get(depth).has(subtotalKey)) {
        byDepth.get(depth).set(subtotalKey, {
          key: subtotalKey,
          depth,
          labels: buildSubtotalDisplayLabels(prefixLabels, depth, n),
          cells: {},
        });
      }
      const subtotal = byDepth.get(depth).get(subtotalKey);
      for (const colHeader of colHeaders) {
        const metrics = cellMap.get(rowHeader.key)?.get(colHeader.key);
        if (!metrics) continue;
        if (!subtotal.cells[colHeader.key]) subtotal.cells[colHeader.key] = {};
        for (const metricKey of valueKeys) {
          const bucket = metrics.get(metricKey);
          const current = subtotal.cells[colHeader.key][metricKey] ?? initMetricBucket();
          if (bucket) {
            current.sum += bucket.sum;
            current.count += bucket.count;
            current.min = current.min == null ? bucket.min : (bucket.min == null ? current.min : Math.min(current.min, bucket.min));
            current.max = current.max == null ? bucket.max : (bucket.max == null ? current.max : Math.max(current.max, bucket.max));
            subtotal.cells[colHeader.key][metricKey] = current;
          }
        }
      }
    }
  }
  /** Row totals for each subtotal (merge across columns), same semantics as rowTotals for detail rows. */
  for (const depthMap of byDepth.values()) {
    for (const subtotal of depthMap.values()) {
      subtotal.rowTotals = {};
      for (const metricKey of valueKeys) {
        const merged = initMetricBucket();
        for (const colHeader of colHeaders) {
          const b = subtotal.cells[colHeader.key]?.[metricKey];
          if (!b) continue;
          merged.sum += b.sum;
          merged.count += b.count;
          merged.min = merged.min == null ? b.min : (b.min == null ? merged.min : Math.min(merged.min, b.min));
          merged.max = merged.max == null ? b.max : (b.max == null ? merged.max : Math.max(merged.max, b.max));
        }
        subtotal.rowTotals[metricKey] = merged;
      }
    }
  }
  const out = [];
  for (const depth of [...byDepth.keys()].sort((a, b) => a - b)) {
    out.push(...byDepth.get(depth).values());
  }
  return out;
}

function assemblePivotFromCellMap(
  normalized,
  values,
  metricKeys,
  cellMap,
  rowHeadersMap,
  colHeadersMap,
  sourceRows,
  filteredRows,
) {
  const rowDir = normalized.sort?.rows === 'desc' ? 'desc' : 'asc';
  const colDir = normalized.sort?.columns === 'desc' ? 'desc' : 'asc';
  const rowHeaders = sortPivotHeaderList([...rowHeadersMap.values()], normalized.rows, rowDir);
  const columnHeaders = sortPivotHeaderList([...colHeadersMap.values()], normalized.columns, colDir);
  if (rowHeaders.length === 0) rowHeaders.push({ key: '(all)', labels: ['(all)'] });
  if (columnHeaders.length === 0) columnHeaders.push({ key: '(all)', labels: ['(all)'] });

  const rowTotals = {};
  const columnTotals = {};
  const grandTotals = {};
  const cells = {};

  for (const rowHeader of rowHeaders) {
    cells[rowHeader.key] = {};
    rowTotals[rowHeader.key] = {};
    for (const metricKey of metricKeys) rowTotals[rowHeader.key][metricKey] = initMetricBucket();
    for (const colHeader of columnHeaders) {
      const metricMap = cellMap.get(rowHeader.key)?.get(colHeader.key);
      cells[rowHeader.key][colHeader.key] = {};
      for (const metricKey of metricKeys) {
        const bucket = metricMap?.get(metricKey) || initMetricBucket();
        cells[rowHeader.key][colHeader.key][metricKey] = bucket;
        rowTotals[rowHeader.key][metricKey].sum += bucket.sum;
        rowTotals[rowHeader.key][metricKey].count += bucket.count;
        rowTotals[rowHeader.key][metricKey].min = rowTotals[rowHeader.key][metricKey].min == null ? bucket.min : (bucket.min == null ? rowTotals[rowHeader.key][metricKey].min : Math.min(rowTotals[rowHeader.key][metricKey].min, bucket.min));
        rowTotals[rowHeader.key][metricKey].max = rowTotals[rowHeader.key][metricKey].max == null ? bucket.max : (bucket.max == null ? rowTotals[rowHeader.key][metricKey].max : Math.max(rowTotals[rowHeader.key][metricKey].max, bucket.max));
        if (!columnTotals[colHeader.key]) columnTotals[colHeader.key] = {};
        if (!columnTotals[colHeader.key][metricKey]) columnTotals[colHeader.key][metricKey] = initMetricBucket();
        columnTotals[colHeader.key][metricKey].sum += bucket.sum;
        columnTotals[colHeader.key][metricKey].count += bucket.count;
        columnTotals[colHeader.key][metricKey].min = columnTotals[colHeader.key][metricKey].min == null ? bucket.min : (bucket.min == null ? columnTotals[colHeader.key][metricKey].min : Math.min(columnTotals[colHeader.key][metricKey].min, bucket.min));
        columnTotals[colHeader.key][metricKey].max = columnTotals[colHeader.key][metricKey].max == null ? bucket.max : (bucket.max == null ? columnTotals[colHeader.key][metricKey].max : Math.max(columnTotals[colHeader.key][metricKey].max, bucket.max));
        if (!grandTotals[metricKey]) grandTotals[metricKey] = initMetricBucket();
        grandTotals[metricKey].sum += bucket.sum;
        grandTotals[metricKey].count += bucket.count;
        grandTotals[metricKey].min = grandTotals[metricKey].min == null ? bucket.min : (bucket.min == null ? grandTotals[metricKey].min : Math.min(grandTotals[metricKey].min, bucket.min));
        grandTotals[metricKey].max = grandTotals[metricKey].max == null ? bucket.max : (bucket.max == null ? grandTotals[metricKey].max : Math.max(grandTotals[metricKey].max, bucket.max));
      }
    }
  }

  const subtotalDepths = (normalized.subtotalFields || [])
    .map((field) => normalized.rows.indexOf(field) + 1)
    .filter((depth) => depth > 0);
  const rowSubtotals = subtotalDepths.length > 0
    ? buildSubtotals(
      rowHeaders,
      columnHeaders,
      cellMap,
      metricKeys,
      normalized.rows.length,
      subtotalDepths,
    )
    : [];

  const visibleCells = rowHeaders.length * columnHeaders.length * metricKeys.length;

  return {
    config: { ...normalized, values },
    values,
    rowHeaders,
    columnHeaders,
    cells,
    rowTotals,
    columnTotals,
    grandTotals,
    rowSubtotals,
    meta: {
      sourceRows,
      filteredRows,
      visibleCells,
    },
    _helpers: { readMetric },
  };
}

async function runPivotWithPostgres(normalized, sqlFilters, values, metricKeys) {
  const { rows: sqlRows, filteredRowCount, execution } = await queryPivotGroupBy(
    normalized,
    pivotPostgresWhereFilters(normalized, sqlFilters),
  );
  const rowHeadersMap = new Map();
  const colHeadersMap = new Map();
  const cellMap = new Map();
  let visibleCellsCapped = false;

  for (const raw of sqlRows) {
    const synthRow = {};
    let ri = 0;
    for (const f of normalized.rows) {
      let v = raw[`dr_${ri}`];
      if (v instanceof Date) v = v.toISOString().slice(0, 10);
      synthRow[f] = v;
      ri += 1;
    }
    let ci = 0;
    for (const f of normalized.columns) {
      let v = raw[`dc_${ci}`];
      if (v instanceof Date) v = v.toISOString().slice(0, 10);
      synthRow[f] = v;
      ci += 1;
    }

    const rowLabels = normalized.rows.map((f) => axisValueFromRow(synthRow, f));
    const colLabels = normalized.columns.map((f) => axisValueFromRow(synthRow, f));
    const rowKey = keyFromParts(rowLabels);
    const colKey = keyFromParts(colLabels);
    const nextRowCount = rowHeadersMap.size + (rowHeadersMap.has(rowKey) ? 0 : 1);
    const nextColCount = colHeadersMap.size + (colHeadersMap.has(colKey) ? 0 : 1);
    if (!isWithinVisibleCellLimit(nextRowCount, nextColCount, metricKeys.length)) {
      visibleCellsCapped = true;
      break;
    }

    if (!rowHeadersMap.has(rowKey)) rowHeadersMap.set(rowKey, { key: rowKey, labels: rowLabels });
    if (!colHeadersMap.has(colKey)) colHeadersMap.set(colKey, { key: colKey, labels: colLabels });

    const metricMap = materializeBucketsFromSqlRow(raw, values);
    if (!cellMap.has(rowKey)) cellMap.set(rowKey, new Map());
    cellMap.get(rowKey).set(colKey, metricMap);
  }

  const assembled = assemblePivotFromCellMap(
    normalized,
    values,
    metricKeys,
    cellMap,
    rowHeadersMap,
    colHeadersMap,
    filteredRowCount,
    filteredRowCount,
  );
  return {
    ...assembled,
    _sqlExecution: execution || { type: 'GROUP_BY' },
    _pivotVisibleCapped: visibleCellsCapped,
  };
}

async function runPivotWithStream(normalized, sqlFilters, memFilters, values, metricKeys) {
  const selectClause = pivotSelectColumns(normalized);
  const agentMapHolder = { map: null };
  const rowHeadersMap = new Map();
  const colHeadersMap = new Map();
  const cellMap = new Map();
  let sourceRows = 0;
  let filteredRows = 0;
  let visibleCellsCapped = false;
  let shouldStop = false;

  for await (const page of streamFilteredSalesPages(selectClause, sqlFilters)) {
    const enrichedPage = await enrichPage(page, agentMapHolder);
    for (const row of enrichedPage) {
      sourceRows += 1;
      if (!memFilters.every((f) => filterMatch(row, f))) continue;
      filteredRows += 1;
      const rowLabels = normalized.rows.map((f) => axisValueFromRow(row, f));
      const colLabels = normalized.columns.map((f) => axisValueFromRow(row, f));
      const rowKey = keyFromParts(rowLabels);
      const colKey = keyFromParts(colLabels);
      const nextRowCount = rowHeadersMap.size + (rowHeadersMap.has(rowKey) ? 0 : 1);
      const nextColCount = colHeadersMap.size + (colHeadersMap.has(colKey) ? 0 : 1);
      if (!isWithinVisibleCellLimit(nextRowCount, nextColCount, metricKeys.length)) {
        visibleCellsCapped = true;
        shouldStop = true;
        break;
      }
      if (!rowHeadersMap.has(rowKey)) rowHeadersMap.set(rowKey, { key: rowKey, labels: rowLabels });
      if (!colHeadersMap.has(colKey)) colHeadersMap.set(colKey, { key: colKey, labels: colLabels });
      const metricMap = ensureCell(cellMap, rowKey, colKey);
      values.forEach((v) => addValue(metricMap, `${v.agg}:${v.field}`, row[v.field], v.field));
    }
    if (shouldStop) break;
  }

  return {
    ...assemblePivotFromCellMap(
    normalized,
    values,
    metricKeys,
    cellMap,
    rowHeadersMap,
    colHeadersMap,
    sourceRows,
    filteredRows,
    ),
    _pivotVisibleCapped: visibleCellsCapped,
  };
}

export function getPivotFields() {
  return SALES_FIELDS.map((field) => ({
    field,
    type: NUMERIC_FIELDS.has(field) ? 'number' : (DATE_FIELDS.has(field) ? 'date' : 'string'),
    group: NUMERIC_FIELDS.has(field) ? 'measure' : 'dimension',
    defaultAgg: NUMERIC_FIELDS.has(field) ? 'sum' : 'count',
  }));
}

export function getPivotCapabilities() {
  return buildPivotCapabilities();
}

export async function runPivot(config = {}) {
  const normalized = normalizeConfig(config);
  assertPivotCardinalityPreflight(normalized);
  if (!normalized.values.length) {
    throw new Error('Add at least one field to Rows, Columns, or Values.');
  }
  const pCacheKey = pivotResultCacheKey(normalized);

  if (isPivotRedisConfigured()) {
    const redisPayload = await pivotRedisGet(pCacheKey);
    if (redisPayload) {
      return { ...redisPayload, _helpers: { readMetric } };
    }
  }

  const cached = pivotResultCache.get(pCacheKey);
  if (cached && Date.now() - cached.t < PIVOT_RESULT_CACHE_TTL_MS) {
    return {
      ...JSON.parse(JSON.stringify(cached.payload)),
      _helpers: { readMetric },
    };
  }
  const inflight = pivotResultInflight.get(pCacheKey);
  if (inflight) {
    const payload = await inflight;
    return {
      ...JSON.parse(JSON.stringify(payload)),
      _helpers: { readMetric },
    };
  }
  const computePromise = (async () => {
  const values = normalized.values;
  const { sqlFilters, memFilters } = splitFilters(normalized.filters);
  const metricKeys = values.map((v) => `${v.agg}:${v.field}`);

  if (String(process.env.PIVOT_DEBUG_ENGINE || '').trim() === '1') {
    console.log('FILTER DEBUG:', { sqlFilters, memFilters });
  }

  const sqlDetails = getPivotSqlAggregationDetails(normalized, memFilters);
  const usePostgresPivot = sqlDetails.eligible;
  const hasPostgresPool = Boolean(getPivotSqlPool());
  if (hasPostgresPool && !usePostgresPivot) {
    logWarn('pivot', 'PIVOT_FALLBACK', {
      reason: sqlDetails.reasons?.[0] || 'postgres_not_eligible',
      reasons: sqlDetails.reasons || [],
      config: normalized,
    });
  }

  if (String(process.env.PIVOT_DEBUG_ENGINE || '').trim() === '1') {
    const dimCount = (normalized.rows?.length || 0) + (normalized.columns?.length || 0);
    console.log('ENGINE CHECK:', {
      hasMemFilters: memFilters.length > 0,
      memFiltersCount: memFilters.length,
      dimensions: dimCount,
      values: normalized.values,
      eligible: usePostgresPivot,
      reasons: sqlDetails.reasons,
    });
    if (!usePostgresPivot) {
      console.log('PIVOT STREAM FALLBACK:', sqlDetails.reasons?.join('; ') || 'unknown');
    }
  }

  const t0 = Date.now();
  const result = usePostgresPivot
    ? await runPivotWithPostgres(normalized, sqlFilters, values, metricKeys)
    : await runPivotWithStream(normalized, sqlFilters, memFilters, values, metricKeys);
  const executionMs = Date.now() - t0;
  const sqlExecution = result?._sqlExecution || null;

  const cardinalityWarnings = getPivotCardinalityWarnings(normalized.rows, normalized.columns);
  const runtimeWarnings = [];
  if (result?._pivotVisibleCapped) {
    runtimeWarnings.push(
      `Pivot output was capped at ${MAX_PIVOT_VISIBLE_CELLS.toLocaleString('en-IN')} visible cells. Add filters for full detail.`,
    );
  }
  if (sqlExecution?.groupRowsCapped) {
    runtimeWarnings.push(
      `Pivot grouped rows were capped at ${(Number(process.env.PIVOT_MAX_GROUP_ROWS) || 80_000).toLocaleString('en-IN')}. Add filters for full detail.`,
    );
  }

  result.meta = {
    ...result.meta,
    engine: usePostgresPivot ? 'postgres' : 'stream',
    executionMs,
    memFiltersCount: memFilters.length,
    ...((cardinalityWarnings.length || runtimeWarnings.length)
      ? { warnings: [...cardinalityWarnings, ...runtimeWarnings] }
      : {}),
    ...(!usePostgresPivot && sqlDetails.reasons?.length
      ? { engineReasons: sqlDetails.reasons, streamFallbackReason: sqlDetails.reasons[0] }
      : {}),
    ...(sqlExecution?.type === 'MV' ? { mv: sqlExecution.mv, mvRelation: sqlExecution.relation } : {}),
  };

  logInfo('pivot', 'PIVOT_EXECUTION', {
    type: sqlExecution?.type || (usePostgresPivot ? 'GROUP_BY' : 'STREAM'),
    mv: sqlExecution?.mv || null,
    duration_ms: executionMs,
    rows: normalized.rows,
    columns: normalized.columns,
    values: normalized.values,
  });

  if (String(process.env.PIVOT_DEBUG_ENGINE || '').trim() === '1') {
    console.log('PIVOT META:', {
      engine: result.meta.engine,
      memFiltersCount: memFilters.length,
      executionMs,
    });
  }

  try {
    const { _helpers: _discard, _sqlExecution: _discardExec, ...payload } = result;
    pivotResultCache.set(pCacheKey, { t: Date.now(), payload });
    if (isPivotRedisConfigured()) {
      void pivotRedisSet(pCacheKey, payload);
    }
    while (pivotResultCache.size > PIVOT_RESULT_CACHE_MAX) {
      const oldest = pivotResultCache.keys().next().value;
      pivotResultCache.delete(oldest);
    }
  } catch {
    /* ignore cache serialization errors */
  }
    return result;
  })();
  pivotResultInflight.set(pCacheKey, computePromise);
  try {
    return await computePromise;
  } finally {
    pivotResultInflight.delete(pCacheKey);
  }
}

export async function runDrilldown(config = {}, drill = {}) {
  const normalized = normalizeConfig(config);
  if (!normalized.values.length) {
    throw new Error('Add at least one field to Rows, Columns, or Values.');
  }
  const { sqlFilters, memFilters } = splitFilters(normalized.filters);
  const offset = Math.max(0, Number(drill.offset) || 0);
  const limit = Math.max(1, Math.min(1000, Number(drill.limit) || 200));

  if (isPivotSqlDrilldownEligible(normalized, memFilters)) {
    try {
      const { total, rows: rawRows } = await queryDrilldownPage(
        normalized,
        pivotPostgresWhereFilters(normalized, sqlFilters),
        drill,
      );
      const agentMapHolder = { map: null };
      const enrichedRows = await enrichPage(rawRows, agentMapHolder);
      return {
        total,
        rows: enrichedRows,
        offset,
        limit,
      };
    } catch (e) {
      logDebug('pivot', 'postgres drilldown fallback to stream', { error: String(e?.message || e) });
    }
  }

  const selectFields = unique([
    ...normalized.rows,
    ...normalized.columns,
    ...normalized.values.map((v) => v.field),
    ...normalized.filters.map((f) => f.field).filter(Boolean),
    ...SALES_FIELDS,
    'agent_name',
  ]);
  const selectClause = unique(['id', ...selectFields]).join(',');

  const agentMapHolder = { map: null };
  let matchIndex = 0;
  const outRows = [];

  for await (const page of streamFilteredSalesPages(selectClause, sqlFilters)) {
    const enrichedPage = await enrichPage(page, agentMapHolder);
    for (const row of enrichedPage) {
      if (!memFilters.every((f) => filterMatch(row, f))) continue;
      const rowLabels = normalized.rows.map((f) => axisValueFromRow(row, f));
      const colLabels = normalized.columns.map((f) => axisValueFromRow(row, f));
      const rowKey = keyFromParts(rowLabels);
      const colKey = keyFromParts(colLabels);
      if (drill.rowKey && drill.rowKey !== rowKey) continue;
      if (drill.columnKey && drill.columnKey !== colKey) continue;
      if (matchIndex >= offset && matchIndex < offset + limit) outRows.push(row);
      matchIndex += 1;
    }
  }

  return {
    total: matchIndex,
    rows: outRows,
    offset,
    limit,
  };
}

/**
 * Half-up rounding to `decimalPlaces` (digit in next position ≥5 rounds up).
 * Uses scaled integer rounding after toPrecision(15) so values like 1.005 round to 1.01 at 2dp
 * instead of wrongly becoming 1.00 from binary float + toFixed.
 */
function roundHalfUpDecimals(value, decimalPlaces) {
  if (value == null || !Number.isFinite(Number(value))) return value;
  const x = Number(value);
  const p = Math.max(0, Math.min(20, Math.floor(decimalPlaces)));
  if (p === 0) {
    const y = Number(x.toPrecision(15));
    return Math.sign(y) * Math.round(Math.abs(y) + 1e-9);
  }
  const factor = 10 ** p;
  const normalized = Number(x.toPrecision(15));
  const scaled = normalized * factor;
  const rounded = Math.sign(scaled) * Math.round(Math.abs(scaled) + 1e-9);
  return rounded / factor;
}

/**
 * Pivot numbers are rounded to whole units (half-up), e.g. 20,06,90,601.56 → 20,06,90,602.
 * Applies to all measures (amounts, qty, rate aggregates, etc.).
 */
export function roundPivotDisplayValue(n, _field) {
  if (n == null || !Number.isFinite(Number(n))) return n;
  return roundHalfUpDecimals(Number(n), 0);
}

export function toDisplayNumber(bucket, agg, metricKey) {
  const raw = readMetric(bucket, agg);
  if (raw == null || typeof raw !== 'number' || !Number.isFinite(raw)) return raw;
  const mk = String(metricKey || '');
  const i = mk.indexOf(':');
  const field = i >= 0 ? mk.slice(i + 1) : '';
  return field ? roundPivotDisplayValue(raw, field) : roundHalfUpDecimals(raw, 0);
}

export async function getFilterValues(field, search = '', limit = '') {
  const cleanField = String(field || '').trim();
  if (!cleanField || !SALES_FIELDS.includes(cleanField)) {
    throw new Error('Invalid filter field');
  }
  const distinct = await queryDistinctPivotFilterValues(cleanField, search, limit);
  if (distinct == null) {
    throw new Error('Unable to fetch distinct filter values from SQL');
  }
  distinct.sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
  return distinct;
}

/**
 * One round-trip for many pivot filter dropdowns (parallel DISTINCT + shared caches / Redis).
 * @returns {Record<string, { values: string[], error?: string }>}
 */
export async function getFilterValuesBatch(rawFields, limit = 500) {
  const raw = Array.isArray(rawFields) ? rawFields : [];
  const list = [...new Set(raw.map((f) => String(f || '').trim()).filter(Boolean))];
  const lim = Math.min(2000, Math.max(50, Number(limit) || 500));
  if (list.length === 0) throw new Error('No filter fields');
  if (list.length > 24) throw new Error('Too many fields (max 24 per batch)');
  const CONCURRENCY = Math.min(10, Math.max(1, Number(process.env.PIVOT_FILTER_BATCH_CONCURRENCY) || 5));
  const queue = new PQueue({ concurrency: CONCURRENCY });
  const entries = await Promise.all(
    list.map((field) =>
      queue.add(async () => {
        if (!SALES_FIELDS.includes(field)) {
          return [field, { values: [], error: 'Invalid filter field' }];
        }
        try {
          const distinct = await queryDistinctPivotFilterValues(field, '', lim);
          if (distinct == null) {
            return [field, { values: [], error: 'Unable to fetch distinct filter values from SQL' }];
          }
          distinct.sort((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }));
          return [field, { values: distinct }];
        } catch (e) {
          return [field, { values: [], error: e?.message || 'Filter values failed' }];
        }
      }),
    ),
  );
  return Object.fromEntries(entries);
}

/**
 * Warm Redis + in-memory DISTINCT caches for common filter fields (optional startup).
 */
export async function preloadCommonPivotFilterCaches() {
  const fields = String(process.env.PIVOT_PRELOAD_FILTER_FIELDS || 'state,branch,brand')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  const lim = Math.min(2000, Math.max(100, Number(process.env.PIVOT_PRELOAD_FILTER_LIMIT) || 500));
  await Promise.all(
    fields.map((field) =>
      queryDistinctPivotFilterValues(field, '', lim).catch(() => null),
    ),
  );
}
