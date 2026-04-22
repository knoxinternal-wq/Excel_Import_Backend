/**
 * Bulk insert into sales_data via PostgreSQL COPY FROM STDIN (CSV).
 * Uses `DATABASE_URL` and shared `pg` pool (dedicated client per COPY). Pooler :6543 → use batch mode (see excelProcessor).
 */
import pgCopyStreams from 'pg-copy-streams';
import { finished } from 'node:stream/promises';
import { assertDatabaseUrl, getPgPool } from '../config/database.js';
import { logError, logInfo, logWarn } from '../utils/logger.js';
import { parseFactNumeric } from '../utils/salesFacts.js';

const copyFrom = pgCopyStreams.from;

/**
 * Supabase/default Postgres often use a short statement_timeout; one large COPY is killed with
 * "canceling statement due to statement timeout".
 * - Unset or IMPORT_STATEMENT_TIMEOUT_MS=0 → SET statement_timeout = 0 (disable for this session).
 * - IMPORT_STATEMENT_TIMEOUT_MS=600000 → 10 minutes (milliseconds).
 * - IMPORT_SYNCHRONOUS_COMMIT_OFF=1 → force synchronous_commit off.
 * - IMPORT_SYNCHRONOUS_COMMIT_STRICT=1 → do not auto-disable sync commit on Supabase hosts.
 */
/**
 * @param {{ verbose?: boolean }} [opts] If verbose, log lines tagged "COPY" (real COPY path only). Pooled job/DDL
 *   clients use the same SETs without spamming the terminal on every progress row batch.
 */
async function applyImportSessionSettings(client, connectionString, opts = {}) {
  const verbose = Boolean(opts.verbose);
  try {
    const t = String(process.env.IMPORT_STATEMENT_TIMEOUT_MS ?? '').trim();
    if (t === '' || t === '0') {
      await client.query('SET statement_timeout = 0');
      if (verbose) logInfo('import', 'COPY session: statement_timeout disabled');
    } else if (/^\d+$/.test(t)) {
      await client.query(`SET statement_timeout = ${t}`);
      if (verbose) logInfo('import', 'COPY session: statement_timeout ms', { ms: Number(t) });
    } else {
      await client.query('SET statement_timeout = 0');
    }
  } catch (e) {
    logError('import', 'SET statement_timeout failed — COPY may hit DB default timeout', {
      message: e?.message,
    });
  }
  try {
    await client.query('SET lock_timeout = 0');
  } catch (e) {
    logWarn('import', 'SET lock_timeout failed', { message: e?.message });
  }
  try {
    await client.query('SET idle_in_transaction_session_timeout = 0');
  } catch {
    /* PG < 14 or permission */
  }
  try {
    await client.query('SET jit = off');
  } catch {
    /* PG without jit */
  }
  const strictSync = String(process.env.IMPORT_SYNCHRONOUS_COMMIT_STRICT || '').trim() === '1';
  const forceOff = String(process.env.IMPORT_SYNCHRONOUS_COMMIT_OFF || '').trim() === '1';
  const looksSupabase = /supabase\.co|pooler\.supabase\.com/i.test(connectionString || '');
  if (forceOff || (!strictSync && looksSupabase)) {
    try {
      await client.query('SET synchronous_commit = off');
      if (verbose) logInfo('import', 'COPY session: synchronous_commit=off (bulk throughput)');
    } catch (e) {
      logWarn('import', 'SET synchronous_commit failed', { message: e?.message });
    }
  }
}

/** Column order must match COPY and CSV line order (same as legacy worker insert shape). */
export const SALES_COPY_COLUMNS = [
  'branch', 'fy', 'month', 'mmm', 'region', 'state', 'district', 'city',
  'business_type', 'agent_names_correction', 'party_grouped', 'party_name_for_count',
  'brand', 'agent_name', 'to_party_name', 'bill_no', 'bill_date', 'item_no', 'shade_name',
  'rate_unit', 'size', 'units_pack', 'sl_qty', 'gross_amount', 'amount_before_tax',
  'net_amount', 'sale_order_no', 'sale_order_date', 'item_with_shade', 'item_category',
  'item_sub_cat', 'so_type', 'scheme', 'goods_type', 'agent_name_final', 'pin_code',
];

const FORCE_NULL_COLUMNS = [];
const DATE_COLUMNS = ['bill_date', 'sale_order_date'];
const DATE_COLS_SET = new Set(DATE_COLUMNS);

const NUMERIC_COLS = new Set([
  'rate_unit', 'sl_qty', 'gross_amount', 'amount_before_tax', 'net_amount',
]);

const IDX_FY = SALES_COPY_COLUMNS.indexOf('fy');
const IDX_MONTH = SALES_COPY_COLUMNS.indexOf('month');
const IDX_MMM = SALES_COPY_COLUMNS.indexOf('mmm');
const IDX_BILL_DATE = SALES_COPY_COLUMNS.indexOf('bill_date');

const CSV_NEED_QUOTE = /[",\n\r]/;
const CSV_DQUOTE = /"/g;

const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

function deriveFYMonthFromBillDate(billDate) {
  if (!billDate) return { fy: null, month: null, mmm: null };
  let d;
  if (billDate instanceof Date) {
    d = billDate;
  } else {
    const s = String(billDate).trim();
    const ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (ymd) d = new Date(parseInt(ymd[1], 10), parseInt(ymd[2], 10) - 1, parseInt(ymd[3], 10));
    else d = new Date(s);
  }
  if (!d || isNaN(d.getTime()) || d.getFullYear() < 2000) return { fy: null, month: null, mmm: null };
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

function toDateString(val) {
  if (!val) return null;
  if (val instanceof Date) {
    if (isNaN(val.getTime())) return null;
    return `${val.getFullYear()}-${String(val.getMonth() + 1).padStart(2, '0')}-${String(val.getDate()).padStart(2, '0')}`;
  }
  const s = String(val).trim();
  if (!s) return null;
  const ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (ymd) return `${ymd[1]}-${ymd[2]}-${ymd[3]}`;
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})T/);
  if (iso) return `${iso[1]}-${iso[2]}-${iso[3]}`;
  return null;
}

/**
 * Build COPY CSV columns into parts[] (reusable buffers). Same semantics as legacy prepareRow + CSV.
 * @returns {{ ok: true } | { ok: false, message: string }}
 */
function factRowToCopyParts(data, tempVals, parts) {
  for (let i = 0; i < SALES_COPY_COLUMNS.length; i++) {
    const col = SALES_COPY_COLUMNS[i];
    let val = FORCE_NULL_COLUMNS.includes(col) ? null : (data[col] ?? null);
    if (val != null && DATE_COLS_SET.has(col)) {
      val = toDateString(val);
    }
    tempVals[i] = val;
  }
  const fyRaw = tempVals[IDX_FY];
  const needsFY = fyRaw == null || String(fyRaw).trim() === '';
  if (needsFY && tempVals[IDX_BILL_DATE]) {
    const derived = deriveFYMonthFromBillDate(tempVals[IDX_BILL_DATE]);
    if (derived.fy) {
      tempVals[IDX_FY] = derived.fy;
      tempVals[IDX_MONTH] = derived.month;
      tempVals[IDX_MMM] = derived.mmm;
    }
  }
  for (let i = 0; i < SALES_COPY_COLUMNS.length; i++) {
    const col = SALES_COPY_COLUMNS[i];
    const v = tempVals[i];
    if (v == null || v === '') {
      parts[i] = '';
      continue;
    }
    if (NUMERIC_COLS.has(col)) {
      const n = typeof v === 'number' && Number.isFinite(v) ? v : parseFactNumeric(v, col);
      if (n == null || !Number.isFinite(n)) return { ok: false, message: `Invalid numeric ${col}` };
      parts[i] = String(n);
      continue;
    }
    parts[i] = escapeCsvField(v);
  }
  return { ok: true };
}

/** CSV field: null → empty (COPY NULL). Quote only when needed. */
export function escapeCsvField(val) {
  if (val == null) return '';
  const s = String(val);
  if (s.length === 0) return '';
  if (CSV_NEED_QUOTE.test(s)) return `"${s.replace(CSV_DQUOTE, '""')}"`;
  return s;
}

/**
 * @returns {{ ok: true, line: string } | { ok: false, message: string }}
 */
export function tryBuildCopyCsvLine(row) {
  try {
    const temp = new Array(SALES_COPY_COLUMNS.length);
    const parts = new Array(SALES_COPY_COLUMNS.length);
    const r = factRowToCopyParts(row, temp, parts);
    if (!r.ok) return r;
    return { ok: true, line: parts.join(',') };
  } catch (e) {
    return { ok: false, message: e?.message || String(e) };
  }
}

export function assertImportDbUrl() {
  return assertDatabaseUrl();
}

function quotedCopyColumnList() {
  return SALES_COPY_COLUMNS.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(', ');
}

function buildCopySql(targetTable) {
  return `COPY ${targetTable} (${quotedCopyColumnList()}) FROM STDIN WITH (FORMAT csv, ENCODING 'UTF8')`;
}

function writeBufferToCopyStream(stream, buf, fatalRef) {
  return new Promise((resolve, reject) => {
    if (fatalRef._fatal) {
      reject(fatalRef._fatal);
      return;
    }
    stream.write(buf, (err) => {
      if (err) reject(err);
      else if (fatalRef._fatal) reject(fatalRef._fatal);
      else resolve();
    });
  });
}

export class SalesCopyWriter {
  /**
   * @param {import('pg').Client} client
   * @param {import('stream').Writable} copyStream
   */
  constructor(client, copyStream) {
    this.client = client;
    this.copyStream = copyStream;
    /** @type {Error | null} */
    this._fatal = null;
    /** @type {string[]} */
    this._lineBatch = [];
    this._lineBatchBytes = 0;
    this._rowsInBatch = 0;
    const envRows = Number(process.env.IMPORT_COPY_ROW_BATCH);
    this._rowBatchThreshold = Number.isFinite(envRows) && envRows >= 1 ? Math.floor(envRows) : 25_000;
    const envBuf = Number(process.env.IMPORT_COPY_BUFFER_BYTES);
    this._flushThreshold = Number.isFinite(envBuf) && envBuf >= 65536 ? envBuf : 64 * 1024 * 1024;
    this._tempVals = new Array(SALES_COPY_COLUMNS.length);
    this._csvParts = new Array(SALES_COPY_COLUMNS.length);
    copyStream.on('error', (e) => {
      this._fatal = e;
    });
  }

  async _flushLineBatch() {
    if (this._lineBatch.length === 0) return;
    const chunk = Buffer.from(`${this._lineBatch.join('\n')}\n`, 'utf8');
    this._lineBatch.length = 0;
    this._lineBatchBytes = 0;
    this._rowsInBatch = 0;
    await writeBufferToCopyStream(this.copyStream, chunk, this);
  }

  /**
   * @returns {{ written: false, skipMessage: string } | { written: true, flushPromise?: Promise<void> }}
   */
  appendRow(row) {
    if (this._fatal) throw this._fatal;
    const r = factRowToCopyParts(row, this._tempVals, this._csvParts);
    if (!r.ok) {
      return { written: false, skipMessage: r.message };
    }
    const line = this._csvParts.join(',');
    this._lineBatch.push(line);
    this._lineBatchBytes += Buffer.byteLength(line, 'utf8') + 1;
    this._rowsInBatch += 1;
    if (this._rowsInBatch >= this._rowBatchThreshold || this._lineBatchBytes >= this._flushThreshold) {
      return { written: true, flushPromise: this._flushLineBatch() };
    }
    return { written: true };
  }

  async complete() {
    if (this._fatal) throw this._fatal;
    await this._flushLineBatch();
    this.copyStream.end();
    await finished(this.copyStream);
    if (this._fatal) throw this._fatal;
  }
}

const GIN_INDEXES = [
  'idx_sales_party_trgm',
  'idx_sales_agent_trgm',
  'idx_sales_item_trgm',
  'idx_sales_bill_no_trgm',
  'idx_sales_party_grouped_trgm',
];

const GIN_DEFS = {
  idx_sales_party_trgm:
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_party_trgm ON sales_data USING gin (to_party_name gin_trgm_ops)',
  idx_sales_agent_trgm:
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_agent_trgm ON sales_data USING gin (agent_name gin_trgm_ops)',
  idx_sales_item_trgm:
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_item_trgm ON sales_data USING gin (item_no gin_trgm_ops)',
  idx_sales_bill_no_trgm:
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_bill_no_trgm ON sales_data USING gin (bill_no gin_trgm_ops)',
  idx_sales_party_grouped_trgm:
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_party_grouped_trgm ON sales_data USING gin (party_grouped gin_trgm_ops)',
};

/**
 * Indian FY partition name from bill date (Apr–Mar), e.g. 2025-04-02 → sales_data_fy_2025_26.
 * @param {string} dateStr
 * @returns {string | null}
 */
export function getPartitionName(dateStr) {
  if (!dateStr) return null;
  const s = String(dateStr).trim();
  const ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!ymd) return null;
  const y = parseInt(ymd[1], 10);
  const m = parseInt(ymd[2], 10);
  if (!Number.isFinite(y) || !Number.isFinite(m)) return null;
  const fyStart = m >= 4 ? y : y - 1;
  return `sales_data_fy_${fyStart}_${String(fyStart + 1).slice(-2)}`;
}

export async function truncateSalesDataStaging(pool) {
  if (!pool) return;
  try {
    await pool.query('TRUNCATE TABLE sales_data_staging');
  } catch (e) {
    logWarn('import', 'staging truncate failed', { message: e?.message });
  }
}

async function rebuildGinIndexesWithPool(pool) {
  if (!pool) return;
  for (const [, ddl] of Object.entries(GIN_DEFS)) {
    try {
      // eslint-disable-next-line no-await-in-loop
      await pool.query(ddl);
    } catch (e) {
      const msg = String(e?.message || '');
      if (/cannot create index on partitioned table .* concurrently/i.test(msg)) {
        const nonConcurrentDdl = ddl.replace(/CREATE INDEX CONCURRENTLY/i, 'CREATE INDEX');
        try {
          // eslint-disable-next-line no-await-in-loop
          await pool.query(nonConcurrentDdl);
          continue;
        } catch (fallbackErr) {
          logWarn('import', 'GIN index rebuild fallback failed', { message: fallbackErr?.message });
          continue;
        }
      }
      logWarn('import', 'GIN index rebuild failed', { message: e?.message });
    }
  }
}

/** Drop heavy GIN indexes on sales_data before bulk COPY (caller may rebuild after). */
export async function dropSalesDataGinIndexes(client) {
  for (const idx of GIN_INDEXES) {
    try {
      // eslint-disable-next-line no-await-in-loop
      await client.query(`DROP INDEX IF EXISTS ${idx}`);
    } catch (e) {
      logWarn('import', `DROP INDEX ${idx} failed`, { message: e?.message });
    }
  }
}

export { rebuildGinIndexesWithPool as rebuildSalesDataGinIndexesPool };

function shouldToggleGinIndexes() {
  return String(process.env.IMPORT_TOGGLE_GIN_INDEXES || '').trim() === '1';
}

export async function withImportDbClient(fn) {
  const pool = getPgPool();
  if (!pool) {
    throw new Error('Excel import needs DATABASE_URL in backend/.env.');
  }
  const connStr = assertImportDbUrl();
  const client = await pool.connect();
  try {
    await applyImportSessionSettings(client, connStr, { verbose: false });
    return await fn(client);
  } finally {
    client.release();
  }
}

export async function ensureImportRuntimeSchema(client) {
  await client.query(`
    ALTER TABLE import_jobs
      ADD COLUMN IF NOT EXISTS checkpoint_row INTEGER DEFAULT 0,
      ADD COLUMN IF NOT EXISTS throughput_rps NUMERIC(12,2) DEFAULT 0,
      ADD COLUMN IF NOT EXISTS worker_id VARCHAR(128),
      ADD COLUMN IF NOT EXISTS queued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;
  `);
  await client.query(`
    ALTER TABLE import_errors
      ADD COLUMN IF NOT EXISTS raw_data JSONB;
  `);
}

/**
 * Connect, start COPY, run async work, end COPY, release client.
 * @param {(writer: SalesCopyWriter) => Promise<void>} fn
 */
export async function withSalesCopyImport(fn, options = {}) {
  const pool = getPgPool();
  if (!pool) {
    throw new Error('Excel import needs DATABASE_URL in backend/.env.');
  }
  const connStr = assertImportDbUrl();
  const targetTable = options.targetTable || 'sales_data';
  const skipGinToggle = Boolean(options.skipGinToggle) || !shouldToggleGinIndexes();
  if (!Boolean(options.skipGinToggle) && skipGinToggle) {
    logInfo('import', 'GIN toggle disabled for COPY (set IMPORT_TOGGLE_GIN_INDEXES=1 to enable)');
  }
  const client = await pool.connect();
  /** @type {import('stream').Writable | null} */
  let copyStream = null;
  let copySucceeded = false;
  client.once('error', (err) => {
    logError('import', 'PostgreSQL client error', { message: err?.message, code: err?.code });
    if (copyStream && !copyStream.destroyed && typeof copyStream.destroy === 'function') {
      try {
        copyStream.destroy(err instanceof Error ? err : new Error(String(err)));
      } catch {
        /* ignore */
      }
    }
  });
  try {
    await applyImportSessionSettings(client, connStr, { verbose: true });
    if (!skipGinToggle) {
      for (const idx of GIN_INDEXES) {
        try {
          // eslint-disable-next-line no-await-in-loop
          await client.query(`DROP INDEX IF EXISTS ${idx}`);
        } catch (e) {
          logWarn('import', `DROP INDEX ${idx} failed`, { message: e?.message });
        }
      }
    }
    copyStream = client.query(copyFrom(buildCopySql(targetTable)));
    const writer = new SalesCopyWriter(client, copyStream);
    try {
      await fn(writer);
      await writer.complete();
      copySucceeded = true;
    } catch (e) {
      try {
        copyStream?.destroy(e instanceof Error ? e : new Error(String(e)));
      } catch {
        /* ignore */
      }
      throw e;
    }
  } finally {
    client.release();
    if (copySucceeded && !skipGinToggle) {
      await rebuildGinIndexesWithPool(pool);
    }
    await truncateSalesDataStaging(pool);
  }
}
