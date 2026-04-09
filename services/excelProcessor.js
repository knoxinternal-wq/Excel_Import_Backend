import ExcelJS from 'exceljs';
import fs from 'fs';
import PQueue from 'p-queue';
import path from 'path';
import { finished } from 'node:stream/promises';
import { normalizeHeader } from '../utils/normalizeHeader.js';
import { v4 as uuidv4 } from 'uuid';
import { supabase } from '../models/supabase.js';
import {
  SKIP_COLUMN_A,
  SKIP_LAST_DATA_ROW,
  REQUIRED_HEADERS,
  NUMERIC_FIELDS,
  DATE_FIELDS,
  ERROR_BATCH_SIZE,
  MAX_SALES_ROWS,
  HEADER_ALIASES,
  HEADER_ALIASES_MULTI,
  EXCLUDED_FROM_IMPORT,
} from '../config/constants.js';
import {
  withImportDbClient,
  ensureImportRuntimeSchema,
  withSalesCopyImport,
  escapeCsvField,
  tryBuildCopyCsvLine,
} from './salesCopyInserter.js';
import { loadImportMastersSnapshot } from './import/importMastersSnapshot.js';
import { importSalesDataFromCsvShards, CSV_IMPORT_HEADERS } from './import/importCsvShardsParallel.js';
import { enrichImportFactRow } from './import/importEnrichFactRow.js';
import { logError, logWarn, logDebug, logInfo } from '../utils/logger.js';
import { parseExcelMoneyQtyCell, parseFactNumeric } from '../utils/salesFacts.js';
import { invalidateMasterCachePrefix } from './masterLookupCache.js';
import { getPgPool } from '../config/database.js';

const NUMERIC_FIELDS_SET = new Set(NUMERIC_FIELDS);
const DATE_FIELDS_SET = new Set(DATE_FIELDS);
const EXCLUDED_FROM_IMPORT_SET = new Set(EXCLUDED_FROM_IMPORT);
const EXCEL_HEADER_MAP = {
  'BRANCH': 'branch',
  'FY': 'fy',
  'MONTH': 'month',
  'MMM': 'mmm',
  'REGION': 'region',
  'STATE': 'state',
  'DISTRICT': 'district',
  'CITY': 'city',
  'TYPE OF Business': 'business_type',
  'Agent Names Correction': 'agent_names_correction',
  'PARTY GROUPED': 'party_grouped',
  'PARTY NAME FOR COUNT': 'party_name_for_count',
  'BRAND': 'brand',
  'AGENT NAME': 'agent_name',
  'TO PARTY NAME': 'to_party_name',
  'BILL NO.': 'bill_no',
  'BILL Date': 'bill_date',
  'ITEM NAME': 'item_no',
  'ITEM NO': 'item_no',
  'ITEMNO': 'item_no',
  'SHADE NAME': 'shade_name',
  'RATE/UNIT': 'rate_unit',
  'SIZE': 'size',
  'UNITS/PACK': 'units_pack',
  'SL QTY': 'sl_qty',
  'GROSS AMOUNT': 'gross_amount',
  'AMOUNT BEFORE TAX': 'amount_before_tax',
  'NET AMOUNT': 'net_amount',
  'SALE ORDER NO.': 'sale_order_no',
  'SALE ORDER Date': 'sale_order_date',
  'Item with Shade': 'item_with_shade',
  'Item Category': 'item_category',
  'PRODUCT': 'item_category',
  'Item Sub cat': 'item_sub_cat',
  'SO TYPE': 'so_type',
  'SCHEME': 'scheme',
  'GOODS TYPE': 'goods_type',
  'AGENT NAME.': 'agent_name_final',
  'PIN CODE': 'pin_code',
};

/** Remove first column (A) from row when SKIP_COLUMN_A is true */
function stripFirstColumn(rowArray, isExcelJsStream = false) {
  if (!SKIP_COLUMN_A || !Array.isArray(rowArray)) return rowArray || [];
  const skip = isExcelJsStream ? 2 : 1; // ExcelJS: col A at index 1; xlsx: col A at index 0
  return rowArray.slice(skip);
}

function resolveRichText(val) {
  if (val && typeof val === 'object' && val.richText && Array.isArray(val.richText)) {
    return val.richText.map((r) => r.text || '').join('');
  }
  return val;
}

function resolveRowValues(rowArray, sharedStrings) {
  if (!sharedStrings || !Array.isArray(sharedStrings)) return rowArray;
  return rowArray.map((cell) => {
    if (cell && typeof cell === 'object' && 'sharedString' in cell) {
      const idx = cell.sharedString;
      const str = sharedStrings[idx];
      return str != null ? resolveRichText(str) : cell;
    }
    return cell;
  });
}

/** Convert any cell value to string for header comparison (handles objects, dates, sharedString refs) */
function cellToString(v) {
  if (v == null) return '';
  if (typeof v === 'string') return v.trim();
  if (typeof v === 'object' && v.sharedString !== undefined) return ''; // Unresolved - skip
  if (typeof v === 'object' && v.richText && Array.isArray(v.richText)) {
    return v.richText.map((r) => r.text || '').join('').trim();
  }
  return String(v).trim();
}

function normalizeHeaderWithDots(header) {
  if (!header) return '';
  return header
    .toString()
    .replace(/\u00A0/g, ' ')
    .trim()
    .replace(/\s+/g, ' ')
    .toUpperCase();
}

/** Resolve Excel header to canonical name using normalizeHeader + aliases.
 * Returns [canonical, normalized] for aliases so both keys get column index (e.g. ITEM NO -> item_no). */
function resolveHeaderToCanonical(rawHeader) {
  const rawNormalized = normalizeHeaderWithDots(rawHeader);
  if (rawNormalized === 'AGENT NAME.') return ['AGENT NAME.'];
  if (rawNormalized === 'AGENT NAME.1') return ['AGENT NAME.'];
  if (rawNormalized === 'AGENT MAP') return ['AGENT NAME.'];

  const normalized = normalizeHeader(rawHeader);
  if (!normalized) return null;
  if (HEADER_ALIASES[normalized]) return [HEADER_ALIASES[normalized], normalized];
  const aliasMultiKey = Object.keys(HEADER_ALIASES_MULTI).find((k) => normalizeHeader(k) === normalized);
  if (aliasMultiKey) return HEADER_ALIASES_MULTI[aliasMultiKey];
  const matchRequired = REQUIRED_HEADERS.find((h) => normalizeHeader(h) === normalized);
  if (matchRequired) return [matchRequired];
  return null;
}

function validateHeaders(headerRow) {
  const detectedCanonical = new Set();
  const cleanRow = headerRow.map((h) => cellToString(h));

  for (const header of cleanRow) {
    const resolved = resolveHeaderToCanonical(header);
    if (resolved) {
      resolved.forEach((h) => detectedCanonical.add(h));
    }
  }

  logDebug('import', 'header row preview', { first15: cleanRow.slice(0, 15), canonical: [...detectedCanonical] });

  const required = [
    'BRANCH',
    'STATE',
    'CITY',
    'BRAND',
    'AGENT NAME',
    'TO PARTY NAME',
    'BILL NO.',
    'BILL Date',
    'SHADE NAME',
    'RATE/UNIT',
    'SIZE',
    'UNITS/PACK',
    'SL QTY',
    'GROSS AMOUNT',
    'AMOUNT BEFORE TAX',
    'NET AMOUNT',
    'SALE ORDER NO.',
    'SALE ORDER Date',
    'Item Category',
    'Item Sub cat',
  ];
  const itemRequired = ['ITEM NAME', 'ITEM NO'];
  const hasItem = itemRequired.some((h) => detectedCanonical.has(h));
  const missing = required.filter((h) => !detectedCanonical.has(h));
  if (!hasItem) missing.push('ITEM NAME or ITEM NO');

  if (missing.length > 0) {
    return { valid: false, missing: missing[0], missingAll: missing, cleanRow };
  }

  return { valid: true, cleanRow };
}

/** Minimum year for date fields - reject older as corrupt (e.g. Excel serial 27 → 1900) */
const MIN_VALID_DATE_YEAR = 2000;

/** Core money/qty columns: clean before insert (commas, ₹, spaces, etc.). `0` must not be treated as blank. */
const IMPORT_TO_NUMBER_HEADERS = new Set([
  'RATE/UNIT',
  'SL QTY',
  'GROSS AMOUNT',
  'AMOUNT BEFORE TAX',
  'NET AMOUNT',
]);

function parseValue(val, type) {
  if (val === null || val === undefined || val === '') return null;
  const str = String(val).trim();
  if (!str) return null;

  if (type === 'number') {
    let num = parseFloat(str.replace(/,/g, ''));
    if (isNaN(num) && /[\d.]+/.test(str)) {
      const match = str.match(/\d+\.?\d*/g);
      if (match) num = parseFloat(match.find((m) => m.includes('.')) || match[0]);
    }
    return isNaN(num) ? null : num;
  }
  if (type === 'date') {
    // Reject dates before year 2000 - almost certainly wrong (e.g. Excel serial 27 → Jan 27, 1900)
    if (val instanceof Date) {
      if (isNaN(val.getTime())) return null;
      if (val.getFullYear() < 2000) return null;
      return val;
    }
    // Try date-string parsing first - avoid misparsing "26-Jan-2025" as Excel serial 26 (Jan 26, 1900)
    const dmY = str.match(/^(\d{1,2})-(\d{1,2})-(\d{4})$/);
    if (dmY) {
      const [, day, month, year] = dmY;
      const d = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
      return isNaN(d.getTime()) ? null : d;
    }
    // DD/MM/YYYY or DD-MM-YYYY (day first - common in India/Europe)
    const dmySlash = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (dmySlash) {
      const [, dPart, mPart, yPart] = dmySlash;
      const day = parseInt(dPart, 10);
      const month = parseInt(mPart, 10) - 1;
      const year = parseInt(yPart, 10);
      const d = new Date(year, month, day);
      return isNaN(d.getTime()) || d.getFullYear() !== year ? null : d;
    }
    const num = parseFloat(str.replace(/,/g, ''));
    if (!isNaN(num)) {
      // Excel serial: only use when clearly a serial (not day-of-month 1-31)
      // Small numbers often come from wrong column (e.g. SHADE NAME) or truncated date strings
      if (num > 31 && num <= 2958465) {
        const isPureNumber = /^\s*\d+(\.\d+)?\s*$/.test(str);
        if (isPureNumber) {
          const excelEpoch = new Date(1899, 11, 30);
          const d = new Date(excelEpoch.getTime() + num * 86400000);
          return isNaN(d.getTime()) ? null : d;
        }
      }
      if (num >= 19000101 && num <= 99991231 && /^\d{8}$/.test(str.trim())) {
        const y = Math.floor(num / 10000);
        const m = Math.floor((num % 10000) / 100) - 1;
        const day = num % 100;
        const d = new Date(y, m, day);
        return isNaN(d.getTime()) || d.getFullYear() !== y ? null : d;
      }
    }
    const d = new Date(str);
    if (!isNaN(d.getTime())) return d;
    // Reject bare small numbers (1-31) - likely wrong column, not a valid date
    const n = parseFloat(str);
    if (!isNaN(n) && n >= 1 && n <= 31 && /^\d+$/.test(str.trim())) return null;
    return null;
  }
  return str;
}

/**
 * Precomputed row mapping: same fields as legacy validateRow, no per-row map iteration.
 * @returns {{ plan: object[], outCols: string[] }}
 */
function buildColumnPlan(headerIndices) {
  const plan = [];
  const outCols = new Set(['item_no']);
  plan.push({
    k: 'item',
    itemNo: headerIndices['ITEM NO'],
    itemName: headerIndices['ITEM NAME'],
  });
  for (const [excelHeader, dbCol] of Object.entries(EXCEL_HEADER_MAP)) {
    if (excelHeader === 'ITEM NAME' || excelHeader === 'ITEM NO') continue;
    if (EXCLUDED_FROM_IMPORT_SET.has(excelHeader)) {
      plan.push({ k: 'null', dbCol });
      outCols.add(dbCol);
      continue;
    }
    const idx = headerIndices[excelHeader];
    if (idx === undefined) {
      // Always reset these per row so derivePartyGrouping sees null (not a stale value from the previous row).
      if (dbCol === 'party_grouped' || dbCol === 'party_name_for_count') {
        outCols.add(dbCol);
        plan.push({ k: 'null', dbCol });
      }
      continue;
    }
    outCols.add(dbCol);
    if (NUMERIC_FIELDS_SET.has(excelHeader)) {
      plan.push({
        k: 'num',
        idx,
        dbCol,
        useToNumber: IMPORT_TO_NUMBER_HEADERS.has(excelHeader),
      });
    } else if (DATE_FIELDS_SET.has(excelHeader)) {
      plan.push({ k: 'date', idx, dbCol });
    } else {
      plan.push({ k: 'text', idx, dbCol });
    }
  }
  return { plan, outCols: Array.from(outCols) };
}

/** Mutates `out` (cleared keys first). Same results as previous validateRow + `{}`. */
function validateRowInto(rowData, columnPlanStruct, out) {
  const { plan, outCols } = columnPlanStruct;
  for (let i = 0; i < outCols.length; i++) {
    out[outCols[i]] = null;
  }
  for (let p = 0; p < plan.length; p++) {
    const step = plan[p];
    if (step.k === 'item') {
      const chosen = step.itemNo !== undefined ? step.itemNo : step.itemName;
      if (chosen !== undefined) {
        const v = rowData[chosen];
        const s = v != null ? String(v).trim() : '';
        out.item_no = s || null;
      } else {
        out.item_no = null;
      }
      continue;
    }
    if (step.k === 'null') {
      out[step.dbCol] = null;
      continue;
    }
    const val = rowData[step.idx];
    const strVal = val != null ? String(val).trim() : '';
    if (step.k === 'num') {
      if (!strVal) {
        out[step.dbCol] = null;
      } else if (step.useToNumber) {
        out[step.dbCol] = parseExcelMoneyQtyCell(val);
      } else if (step.dbCol === 'units_pack') {
        out[step.dbCol] = parseFactNumeric(val, 'units_pack');
      } else {
        out[step.dbCol] = parseFactNumeric(val, step.dbCol);
      }
      continue;
    }
    if (step.k === 'date') {
      out[step.dbCol] = strVal ? parseValue(val, 'date') : null;
      continue;
    }
    out[step.dbCol] = strVal || null;
  }
}

/**
 * Prefer explicit "SL QTY" over generic "QTY" when both columns resolve to SL QTY (last-column-wins would pick the wrong one).
 */
function slQtyHeaderPreferenceScore(rawHeader) {
  const u = normalizeHeaderWithDots(rawHeader);
  if (!u) return 0;
  if (u.includes('SL') && (u.includes('QTY') || u.includes('QUANT'))) return 100;
  if (u.includes('SEL') && u.includes('QTY')) return 80;
  if (u === 'QTY' || u === 'QTY.' || u.startsWith('QTY ')) return 25;
  return 50;
}

function buildHeaderIndices(headerRow) {
  const indicesByCanonical = {};
  let slQtyBest = { index: -1, score: -1 };
  headerRow.forEach((header, index) => {
    const normalized = normalizeHeader(header);
    if (!normalized) return;
    const resolved = resolveHeaderToCanonical(header);
    if (!resolved) return;
    resolved.forEach((canonical) => {
      if (canonical === 'SL QTY') {
        const score = slQtyHeaderPreferenceScore(header);
        if (
          score > slQtyBest.score
          || (score === slQtyBest.score && (slQtyBest.index < 0 || index < slQtyBest.index))
        ) {
          slQtyBest = { index, score };
        }
        return;
      }
      indicesByCanonical[canonical] = index;
    });
  });
  if (slQtyBest.index >= 0) {
    indicesByCanonical['SL QTY'] = slQtyBest.index;
  }
  logDebug('import', 'header indices built', { canonicalCount: Object.keys(indicesByCanonical).length });
  return indicesByCanonical;
}

/** Check if job was cancelled (reads from DB - no in-memory storage) */
async function isJobCancelled(jobId) {
  const pool = getPgPool();
  if (pool) {
    try {
      const { rows } = await pool.query(
        'SELECT cancelled FROM import_jobs WHERE id = $1 LIMIT 1',
        [jobId],
      );
      return rows[0]?.cancelled === true;
    } catch {
      /* fall back to REST */
    }
  }
  const { data, error } = await supabase
    .from('import_jobs')
    .select('cancelled')
    .eq('id', jobId)
    .single();
  return !error && data?.cancelled === true;
}

/** Never call isJobCancelled per row — that is one HTTP request per row (e.g. 400k requests per file). */
const IMPORT_CANCEL_CHECK_EVERY_ROWS =
  Number(process.env.IMPORT_CANCEL_CHECK_EVERY_ROWS) > 0
    ? Math.max(200, Math.floor(Number(process.env.IMPORT_CANCEL_CHECK_EVERY_ROWS)))
    : 2500;

/** How many processed rows between import_jobs progress writes (lower = fresher UI, more DB round-trips). */
const JOB_UPDATE_EVERY_N_ROWS =
  Number(process.env.IMPORT_JOB_UPDATE_EVERY_ROWS) > 0
    ? Math.max(5000, Math.floor(Number(process.env.IMPORT_JOB_UPDATE_EVERY_ROWS)))
    : 100_000;

/** Parallel CSV shards + concurrent COPY sessions (import tab only). Set to 1 to use single-stream COPY. */
const IMPORT_COPY_PARALLEL =
  Number(process.env.IMPORT_COPY_PARALLEL) > 0
    ? Math.min(8, Math.max(1, Math.floor(Number(process.env.IMPORT_COPY_PARALLEL))))
    : 4;

/**
 * When true (default): single COPY stream only — if import fails, Postgres rolls back that COPY (no partial rows).
 * Set IMPORT_ATOMIC=0 to allow parallel shards (faster; a shard failure can leave rows from other shards committed).
 */
function importAtomicEnabled() {
  return String(process.env.IMPORT_ATOMIC ?? '1').trim() !== '0';
}

function useParallelShardImport() {
  if (importAtomicEnabled()) return false;
  return Boolean(getPgPool()) && IMPORT_COPY_PARALLEL >= 2;
}
const SUPABASE_IMPORT_BATCH_SIZE =
  Number(process.env.IMPORT_SUPABASE_BATCH_SIZE) > 0
    ? Math.min(2000, Math.floor(Number(process.env.IMPORT_SUPABASE_BATCH_SIZE)))
    : 1000;
const SUPABASE_IMPORT_BATCH_CONCURRENCY =
  Number(process.env.IMPORT_SUPABASE_BATCH_CONCURRENCY) > 0
    ? Math.min(12, Math.floor(Number(process.env.IMPORT_SUPABASE_BATCH_CONCURRENCY)))
    : 3;
const SUPABASE_IMPORT_TIMEOUT_MIN_BATCH =
  Number(process.env.IMPORT_SUPABASE_TIMEOUT_MIN_BATCH) > 0
    ? Math.max(50, Math.floor(Number(process.env.IMPORT_SUPABASE_TIMEOUT_MIN_BATCH)))
    : 120;
const SUPABASE_IMPORT_RETRY_MAX =
  Number(process.env.IMPORT_SUPABASE_RETRY_MAX) > 0
    ? Math.min(6, Math.floor(Number(process.env.IMPORT_SUPABASE_RETRY_MAX)))
    : 3;

function detectImportMode() {
  const envMode = String(process.env.IMPORT_INSERT_MODE || '').trim().toLowerCase();
  if (envMode === 'postgres_copy' || envMode === 'supabase_batch') return envMode;
  const dbUrl = String(process.env.DATABASE_URL || '').toLowerCase();
  // Supabase pooler (6543 / pooler host) does not reliably support COPY FROM STDIN for this workflow.
  if (dbUrl.includes('pooler.supabase.com') || dbUrl.includes(':6543')) return 'supabase_batch';
  return 'postgres_copy';
}

async function withSupabaseBatchImport(runner) {
  const pendingRows = [];
  const inflight = new Set();
  const isStatementTimeoutErr = (error) => {
    const msg = String(error?.message || '').toLowerCase();
    return msg.includes('statement timeout') || msg.includes('canceling statement due to statement timeout');
  };
  const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const insertRowsWithRetry = async (rows, attempt = 0) => {
    if (!rows.length) return;
    const { error } = await supabase.from('sales_data').insert(rows, { ignoreDuplicates: false });
    if (!error) return;
    const shouldSplit = isStatementTimeoutErr(error) && rows.length > SUPABASE_IMPORT_TIMEOUT_MIN_BATCH;
    const shouldRetry = attempt < SUPABASE_IMPORT_RETRY_MAX;
    if (shouldSplit || shouldRetry) {
      if (shouldSplit) {
        const mid = Math.floor(rows.length / 2);
        await insertRowsWithRetry(rows.slice(0, mid), attempt + 1);
        await insertRowsWithRetry(rows.slice(mid), attempt + 1);
        return;
      }
      await sleep(120 * (attempt + 1));
      await insertRowsWithRetry(rows, attempt + 1);
      return;
    }
    throw new Error(`Supabase batch insert failed: ${error.message}`);
  };
  const flushOneBatch = async () => {
    if (pendingRows.length === 0) return false;
    const batch = pendingRows.splice(0, SUPABASE_IMPORT_BATCH_SIZE);
    await insertRowsWithRetry(batch, 0);
    return pendingRows.length > 0;
  };
  const launchFlush = () => {
    if (pendingRows.length < SUPABASE_IMPORT_BATCH_SIZE) return null;
    const p = flushOneBatch()
      .finally(() => inflight.delete(p));
    inflight.add(p);
    return p;
  };
  const writer = {
    appendRow(data) {
      // `data` is reused row-to-row; clone for async batch insert.
      pendingRows.push({ ...data });
      if (pendingRows.length >= SUPABASE_IMPORT_BATCH_SIZE) {
        // Start async fixed-size DB writes; only backpressure when too many concurrent batches are in flight.
        while (pendingRows.length >= SUPABASE_IMPORT_BATCH_SIZE
          && inflight.size < SUPABASE_IMPORT_BATCH_CONCURRENCY) {
          launchFlush();
        }
        if (inflight.size >= SUPABASE_IMPORT_BATCH_CONCURRENCY) {
          return { written: true, flushPromise: Promise.race(Array.from(inflight)) };
        }
      }
      return { written: true };
    },
    async finish() {
      while (pendingRows.length > 0 || inflight.size > 0) {
        while (pendingRows.length >= SUPABASE_IMPORT_BATCH_SIZE
          && inflight.size < SUPABASE_IMPORT_BATCH_CONCURRENCY) {
          launchFlush();
        }
        if (pendingRows.length > 0 && inflight.size === 0) {
          // Final tail batch (< batch size).
          // eslint-disable-next-line no-await-in-loop
          await flushOneBatch();
          continue;
        }
        if (inflight.size > 0) {
          // eslint-disable-next-line no-await-in-loop
          await Promise.race(Array.from(inflight));
        }
      }
    },
  };
  await runner(writer);
  await writer.finish();
}

function slimRowForErrorLog(data) {
  return {
    bill_no: data?.bill_no ?? null,
    bill_date: data?.bill_date != null ? String(data.bill_date) : null,
    item_no: data?.item_no ?? null,
    to_party_name: data?.to_party_name ?? null,
  };
}

export async function processExcelFile(filePath, filename, fileSize) {
  await withImportDbClient(async (client) => {
    await ensureImportRuntimeSchema(client);
  });
  const jobId = uuidv4();
  const job = {
    jobId,
    filePath,
    filename,
    fileSize,
    totalRows: 0,
    processedRows: 0,
    failedRows: 0,
    status: 'queued',
    queuedAt: new Date(),
    startedAt: null,
    throughputRps: 0,
    checkpointRow: 0,
    cancelled: false,
    importCapped: false,
  };
  await updateJobInDb(jobId, job);
  importJobContext.set(jobId, { filePath, filename, fileSize });
  importJobQueue.add(() => runQueuedImportJob(job)).catch((err) => {
    logError('import', 'queue add failed', { jobId, message: err?.message });
  });
  return jobId;
}

export async function resumeQueuedImport(jobId) {
  await withImportDbClient(async (client) => {
    await ensureImportRuntimeSchema(client);
  });
  const ctx = importJobContext.get(jobId);
  if (!ctx) throw new Error('Resume unavailable: file context not found');
  const { data: row, error } = await supabase
    .from('import_jobs')
    .select('*')
    .eq('id', jobId)
    .single();
  if (error || !row) throw new Error('Job not found');
  const job = {
    jobId,
    filePath: ctx.filePath,
    filename: row.filename || ctx.filename,
    fileSize: row.file_size || ctx.fileSize,
    totalRows: row.total_rows || 0,
    processedRows: row.processed_rows || 0,
    failedRows: row.failed_rows || 0,
    status: 'queued',
    queuedAt: new Date(),
    startedAt: null,
    checkpointRow: row.checkpoint_row || 0,
    throughputRps: 0,
    cancelled: false,
    importCapped: false,
  };
  await updateJobInDb(jobId, job);
  importJobQueue.add(() => runQueuedImportJob(job)).catch((err) => {
    logError('import', 'resume queue add failed', { jobId, message: err?.message });
  });
}

const IMPORT_QUEUE_CONCURRENCY = Number(process.env.IMPORT_WORKER_CONCURRENCY) > 0
  ? Math.floor(Number(process.env.IMPORT_WORKER_CONCURRENCY))
  : 5;
const importJobQueue = new PQueue({ concurrency: IMPORT_QUEUE_CONCURRENCY });
const importJobContext = new Map();

function writeShardChunk(ws, chunk) {
  return new Promise((resolve, reject) => {
    if (ws.destroyed) {
      reject(new Error('Shard write stream destroyed'));
      return;
    }
    const ok = ws.write(chunk, 'utf8', (err) => {
      if (err) reject(err);
    });
    if (ok) resolve();
    else ws.once('drain', resolve);
  });
}

/**
 * One streaming pass over the workbook: validate rows, emit CSV shards (round-robin) for parallel COPY.
 */
async function streamExcelToCsvShards({
  job,
  filePath,
  checkpointStart,
  numShards,
  errorBuffer,
  startedAtMs,
  importProcessedBaseline,
}) {
  const tmpDir = path.join(path.dirname(filePath), 'import-tmp', job.jobId);
  /** @type {import('fs').WriteStream[]} */
  let streams = [];
  let shardPaths = [];
  await fs.promises.mkdir(tmpDir, { recursive: true });
  shardPaths = Array.from({ length: numShards }, (_, i) => path.join(tmpDir, `part-${i}.csv`));
  streams = shardPaths.map((p) =>
    fs.createWriteStream(p, { flags: 'w', highWaterMark: 4 * 1024 * 1024 }),
  );

  let columnPlanStruct = null;
  const rowDataOut = {};
  let totalDataRowsSeen = 0;
  let csvRowsWritten = 0;
  let rowsSinceCancelCheck = 0;
  let rowsSinceStreamPersist = 0;
  let streamOk = false;
  /** When SKIP_LAST_DATA_ROW: hold one data row until the next arrives; final row per workbook is dropped after the loop. */
  let pendingRowArray = null;
  let pendingRowNumber = null;

  try {
    await writeShardChunk(
      streams[0],
      `${CSV_IMPORT_HEADERS.map(escapeCsvField).join(',')}\n`,
    );

    const flushPendingShardRow = async () => {
      if (pendingRowArray == null || !columnPlanStruct) return;
      const fromRowArray = pendingRowArray;
      const fromRowNumber = pendingRowNumber;
      pendingRowArray = null;
      pendingRowNumber = null;
      validateRowInto(fromRowArray, columnPlanStruct, rowDataOut);
      const out = { ...rowDataOut };
      const built = tryBuildCopyCsvLine(out);
      if (!built.ok) {
        job.failedRows += 1;
        errorBuffer.push({
          rowNumber: fromRowNumber,
          rowData: slimRowForErrorLog(out),
          errorMessage: built.message || 'Invalid row for COPY',
        });
      } else {
        const shardIdx = csvRowsWritten % numShards;
        await writeShardChunk(streams[shardIdx], `${fromRowNumber},${built.line}\n`);
        csvRowsWritten += 1;
        job.checkpointRow = fromRowNumber;
      }
      if (errorBuffer.length >= ERROR_BATCH_SIZE) {
        const toInsert = errorBuffer.splice(0, errorBuffer.length);
        await saveErrorRows(job.jobId, toInsert);
      }
      if (rowsSinceStreamPersist >= JOB_UPDATE_EVERY_N_ROWS) {
        rowsSinceStreamPersist = 0;
        job.processedRows = importProcessedBaseline;
        const elapsedSec = Math.max((Date.now() - startedAtMs) / 1000, 1);
        job.throughputRps = Number((totalDataRowsSeen / elapsedSec).toFixed(2));
        await updateJobInDb(job.jobId, job);
      }
    };

    const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {
      sharedStrings: 'cache',
      worksheets: 'emit',
      hyperlinks: 'ignore',
      styles: 'cache',
    });

    outer: for await (const worksheetReader of workbook) {
      pendingRowArray = null;
      pendingRowNumber = null;
      const sharedStrings = workbook.sharedStrings || worksheetReader.workbook?.sharedStrings;
      let headerValidated = false;
      let headerAttemptCount = 0;
      const MAX_HEADER_ATTEMPTS_AFTER_SKIP = 30;
      for await (const row of worksheetReader) {
        rowsSinceCancelCheck += 1;
        if (rowsSinceCancelCheck >= IMPORT_CANCEL_CHECK_EVERY_ROWS) {
          rowsSinceCancelCheck = 0;
          if (await isJobCancelled(job.jobId)) {
            job.cancelled = true;
            await flushPendingShardRow();
            break outer;
          }
        }
        const rowValues = row.values || [];
        let rowArray = Array.isArray(rowValues) ? rowValues : [...rowValues];
        rowArray = resolveRowValues(rowArray, sharedStrings);
        if (SKIP_COLUMN_A) rowArray = rowArray.slice(2);

        if (!headerValidated) {
          headerAttemptCount += 1;
          if (headerAttemptCount > MAX_HEADER_ATTEMPTS_AFTER_SKIP) {
            throw new Error('Invalid headers: required header row not found after skips.');
          }
          const candidate = validateHeaders(rowArray);
          if (!candidate.valid) continue;
          headerValidated = true;
          const hi = buildHeaderIndices(rowArray);
          columnPlanStruct = buildColumnPlan(hi);
          continue;
        }

        totalDataRowsSeen += 1;
        const rowNumber = totalDataRowsSeen + 1;
        if (rowNumber <= checkpointStart) {
          continue;
        }

        rowsSinceStreamPersist += 1;

        if (SKIP_LAST_DATA_ROW) {
          if (pendingRowArray !== null) {
            if (csvRowsWritten >= MAX_SALES_ROWS) {
              job.importCapped = true;
              pendingRowArray = null;
              pendingRowNumber = null;
              break outer;
            }
            await flushPendingShardRow();
          }
          if (csvRowsWritten >= MAX_SALES_ROWS) {
            job.importCapped = true;
            pendingRowArray = null;
            pendingRowNumber = null;
            break outer;
          }
          pendingRowArray = rowArray;
          pendingRowNumber = rowNumber;
        } else {
          if (csvRowsWritten >= MAX_SALES_ROWS) {
            job.importCapped = true;
            break outer;
          }

          validateRowInto(rowArray, columnPlanStruct, rowDataOut);
          const out = { ...rowDataOut };

          const built = tryBuildCopyCsvLine(out);
          if (!built.ok) {
            job.failedRows += 1;
            errorBuffer.push({
              rowNumber,
              rowData: slimRowForErrorLog(out),
              errorMessage: built.message || 'Invalid row for COPY',
            });
          } else {
            const shardIdx = csvRowsWritten % numShards;
            await writeShardChunk(streams[shardIdx], `${rowNumber},${built.line}\n`);
            csvRowsWritten += 1;
            job.checkpointRow = rowNumber;
          }

          if (errorBuffer.length >= ERROR_BATCH_SIZE) {
            const toInsert = errorBuffer.splice(0, errorBuffer.length);
            await saveErrorRows(job.jobId, toInsert);
          }

          if (rowsSinceStreamPersist >= JOB_UPDATE_EVERY_N_ROWS) {
            rowsSinceStreamPersist = 0;
            job.processedRows = importProcessedBaseline;
            const elapsedSec = Math.max((Date.now() - startedAtMs) / 1000, 1);
            job.throughputRps = Number((totalDataRowsSeen / elapsedSec).toFixed(2));
            await updateJobInDb(job.jobId, job);
          }
        }
      }
    }

    pendingRowArray = null;
    pendingRowNumber = null;

    job.processedRows = importProcessedBaseline;
    const elapsedSec = Math.max((Date.now() - startedAtMs) / 1000, 1);
    job.throughputRps = Number((totalDataRowsSeen / elapsedSec).toFixed(2));
    await updateJobInDb(job.jobId, job);

    streamOk = true;
    return { shardPaths, tmpDir, totalDataRowsSeen, csvRowsWritten };
  } finally {
    for (const s of streams) {
      try {
        s.end();
      } catch {
        /* ignore */
      }
    }
    await Promise.all(
      streams.map((s) =>
        finished(s).catch(() => {
          /* ignore */
        }),
      ),
    );
    if (!streamOk) {
      await fs.promises.rm(tmpDir, { recursive: true, force: true }).catch(() => {
        /* ignore */
      });
    }
  }
}

/**
 * Import path for the import tab: parallel shard CSVs + concurrent COPY (when pool exists and IMPORT_COPY_PARALLEL>=2),
 * otherwise single Excel stream + one COPY (same enrichment / validation logic).
 */
async function importExcelForQueuedJob({
  job,
  filePath,
  checkpointStart,
  errorBuffer,
  startedAtMs,
  importProcessedBaseline,
  masters,
}) {
  if (!useParallelShardImport()) {
    return importExcelDirectToSalesData({
      job,
      filePath,
      checkpointStart,
      errorBuffer,
      startedAtMs,
      importProcessedBaseline,
      masters,
    });
  }

  logInfo('import', 'using parallel CSV shards + COPY', { shards: IMPORT_COPY_PARALLEL, jobId: job.jobId });
  const streamResult = await streamExcelToCsvShards({
    job,
    filePath,
    checkpointStart,
    numShards: IMPORT_COPY_PARALLEL,
    errorBuffer,
    startedAtMs,
    importProcessedBaseline,
  });

  if (job.cancelled || streamResult.csvRowsWritten === 0) {
    await fs.promises.rm(streamResult.tmpDir, { recursive: true, force: true }).catch(() => {
      /* ignore */
    });
    return {
      totalDataRowsSeen: streamResult.totalDataRowsSeen,
      rowsWritten: 0,
    };
  }

  try {
    await importSalesDataFromCsvShards({
      shardPaths: streamResult.shardPaths,
      job,
      masters,
      isJobCancelled,
      updateJobInDb,
      jobUpdateEveryRows: JOB_UPDATE_EVERY_N_ROWS,
      saveErrorRows,
      errorBatchSize: ERROR_BATCH_SIZE,
      startedAtMs,
      processedRowsBaseline: importProcessedBaseline,
      failedRowsBaseline: job.failedRows,
    });
  } finally {
    await fs.promises.rm(streamResult.tmpDir, { recursive: true, force: true }).catch(() => {
      /* ignore */
    });
  }

  const rowsWritten = Math.max(0, Number(job.processedRows || 0) - importProcessedBaseline);
  return {
    totalDataRowsSeen: streamResult.totalDataRowsSeen,
    rowsWritten,
  };
}

async function importExcelDirectToSalesData({
  job,
  filePath,
  checkpointStart,
  errorBuffer,
  startedAtMs,
  importProcessedBaseline,
  masters,
}) {
  let columnPlanStruct = null;
  const rowDataOut = {};
  let totalDataRowsSeen = 0;
  let rowsWritten = 0;
  let rowsSinceCancelCheck = 0;
  let rowsSincePersist = 0;
  let pendingRowArray = null;
  let pendingRowNumber = null;

  await withSalesCopyImport(async (writer) => {
    const flushPendingDirectRow = async () => {
      if (pendingRowArray == null || !columnPlanStruct) return;
      const fromRowArray = pendingRowArray;
      const fromRowNumber = pendingRowNumber;
      pendingRowArray = null;
      pendingRowNumber = null;
      validateRowInto(fromRowArray, columnPlanStruct, rowDataOut);
      try {
        enrichImportFactRow(rowDataOut, masters);
        const writeResult = writer.appendRow(rowDataOut);
        if (!writeResult.written) {
          job.failedRows += 1;
          errorBuffer.push({
            rowNumber: fromRowNumber,
            rowData: slimRowForErrorLog(rowDataOut),
            errorMessage: writeResult.skipMessage || 'Row skipped for COPY',
          });
        } else {
          rowsWritten += 1;
          job.checkpointRow = fromRowNumber;
          if (writeResult.flushPromise) await writeResult.flushPromise;
        }
      } catch (e) {
        job.failedRows += 1;
        errorBuffer.push({
          rowNumber: fromRowNumber,
          rowData: slimRowForErrorLog(rowDataOut),
          errorMessage: e?.message || String(e),
        });
      }
      if (errorBuffer.length >= ERROR_BATCH_SIZE) {
        const toInsert = errorBuffer.splice(0, errorBuffer.length);
        await saveErrorRows(job.jobId, toInsert);
      }
      if (rowsSincePersist >= JOB_UPDATE_EVERY_N_ROWS) {
        rowsSincePersist = 0;
        job.processedRows = importProcessedBaseline + rowsWritten;
        const elapsedSec = Math.max((Date.now() - startedAtMs) / 1000, 1);
        job.throughputRps = Number((job.processedRows / elapsedSec).toFixed(2));
        await updateJobInDb(job.jobId, job);
      }
    };

    const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath, {
      sharedStrings: 'cache',
      worksheets: 'emit',
      hyperlinks: 'ignore',
      styles: 'cache',
    });

    outer: for await (const worksheetReader of workbook) {
      pendingRowArray = null;
      pendingRowNumber = null;
      const sharedStrings = workbook.sharedStrings || worksheetReader.workbook?.sharedStrings;
      let headerValidated = false;
      let headerAttemptCount = 0;
      const MAX_HEADER_ATTEMPTS_AFTER_SKIP = 30;
      for await (const row of worksheetReader) {
        rowsSinceCancelCheck += 1;
        if (rowsSinceCancelCheck >= IMPORT_CANCEL_CHECK_EVERY_ROWS) {
          rowsSinceCancelCheck = 0;
          if (await isJobCancelled(job.jobId)) {
            job.cancelled = true;
            await flushPendingDirectRow();
            break outer;
          }
        }

        const rowValues = row.values || [];
        let rowArray = Array.isArray(rowValues) ? rowValues : [...rowValues];
        rowArray = resolveRowValues(rowArray, sharedStrings);
        if (SKIP_COLUMN_A) rowArray = rowArray.slice(2);

        if (!headerValidated) {
          headerAttemptCount += 1;
          if (headerAttemptCount > MAX_HEADER_ATTEMPTS_AFTER_SKIP) {
            throw new Error('Invalid headers: required header row not found after skips.');
          }
          const candidate = validateHeaders(rowArray);
          if (!candidate.valid) continue;
          headerValidated = true;
          const hi = buildHeaderIndices(rowArray);
          columnPlanStruct = buildColumnPlan(hi);
          continue;
        }

        totalDataRowsSeen += 1;
        const rowNumber = totalDataRowsSeen + 1;
        if (rowNumber <= checkpointStart) {
          continue;
        }

        rowsSincePersist += 1;

        if (SKIP_LAST_DATA_ROW) {
          if (pendingRowArray !== null) {
            if (rowsWritten >= MAX_SALES_ROWS) {
              job.importCapped = true;
              pendingRowArray = null;
              pendingRowNumber = null;
              break outer;
            }
            await flushPendingDirectRow();
          }
          if (rowsWritten >= MAX_SALES_ROWS) {
            job.importCapped = true;
            pendingRowArray = null;
            pendingRowNumber = null;
            break outer;
          }
          pendingRowArray = rowArray;
          pendingRowNumber = rowNumber;
        } else {
          if (rowsWritten >= MAX_SALES_ROWS) {
            job.importCapped = true;
            break outer;
          }

          validateRowInto(rowArray, columnPlanStruct, rowDataOut);

          try {
            enrichImportFactRow(rowDataOut, masters);
            const writeResult = writer.appendRow(rowDataOut);
            if (!writeResult.written) {
              job.failedRows += 1;
              errorBuffer.push({
                rowNumber,
                rowData: slimRowForErrorLog(rowDataOut),
                errorMessage: writeResult.skipMessage || 'Row skipped for COPY',
              });
            } else {
              rowsWritten += 1;
              job.checkpointRow = rowNumber;
              if (writeResult.flushPromise) await writeResult.flushPromise;
            }
          } catch (e) {
            job.failedRows += 1;
            errorBuffer.push({
              rowNumber,
              rowData: slimRowForErrorLog(rowDataOut),
              errorMessage: e?.message || String(e),
            });
          }

          if (errorBuffer.length >= ERROR_BATCH_SIZE) {
            const toInsert = errorBuffer.splice(0, errorBuffer.length);
            await saveErrorRows(job.jobId, toInsert);
          }

          if (rowsSincePersist >= JOB_UPDATE_EVERY_N_ROWS) {
            rowsSincePersist = 0;
            job.processedRows = importProcessedBaseline + rowsWritten;
            const elapsedSec = Math.max((Date.now() - startedAtMs) / 1000, 1);
            job.throughputRps = Number((job.processedRows / elapsedSec).toFixed(2));
            await updateJobInDb(job.jobId, job);
          }
        }
      }
    }

    pendingRowArray = null;
    pendingRowNumber = null;
  });

  job.processedRows = importProcessedBaseline + rowsWritten;
  const elapsedSec = Math.max((Date.now() - startedAtMs) / 1000, 1);
  job.throughputRps = Number((job.processedRows / elapsedSec).toFixed(2));
  await updateJobInDb(job.jobId, job);

  return { totalDataRowsSeen, rowsWritten };
}

async function runQueuedImportJob(job) {
  const startedAtMs = Date.now();
  const filePath = job.filePath;
  const importProcessedBaseline = Number(job.processedRows || 0);
  try {
    if (await isJobCancelled(job.jobId)) {
      job.cancelled = true;
      job.status = 'cancelled';
      job.completedAt = new Date();
      await updateJobInDb(job.jobId, job);
      return;
    }
    job.status = 'processing';
    job.startedAt = new Date();
    await updateJobInDb(job.jobId, job);

    await withImportDbClient(async (client) => {
      await ensureImportRuntimeSchema(client);
    });

    const errorBuffer = [];
    const checkpointStart = Number(job.checkpointRow || 0);

    const masters = await loadImportMastersSnapshot();
    const importResult = await importExcelForQueuedJob({
      job,
      filePath,
      checkpointStart,
      errorBuffer,
      startedAtMs,
      importProcessedBaseline,
      masters,
    });

    job.totalRows = importResult.totalDataRowsSeen;

    if (errorBuffer.length) await saveErrorRows(job.jobId, errorBuffer);

    if (job.cancelled) {
      if (errorBuffer.length) await saveErrorRows(job.jobId, errorBuffer);
      job.status = 'cancelled';
      job.completedAt = new Date();
      await updateJobInDb(job.jobId, job);
      return;
    }

    if (importResult.rowsWritten === 0) {
      job.status = 'completed';
      job.completedAt = new Date();
      await updateJobInDb(job.jobId, job);
      invalidateMasterCachePrefix('sales_data_count');
      return;
    }

    job.status = 'completed';
    invalidateMasterCachePrefix('sales_data_count');
    job.completedAt = new Date();
    await updateJobInDb(job.jobId, job);
  } catch (err) {
    logError('import', 'import failed', { jobId: job.jobId, message: err?.message, stack: err?.stack });
    job.status = 'failed';
    job.error = err?.message || String(err);
    job.completedAt = new Date();
    await updateJobInDb(job.jobId, job);
  } finally {
    if (job.status !== 'cancelled') {
      try {
        await fs.promises.unlink(filePath);
      } catch {
        /* ignore */
      }
      importJobContext.delete(job.jobId);
    }
  }
}

async function updateJobInDb(jobId, job) {
  try {
    await withImportDbClient(async (client) => {
      // Schema ensured at job start / upload — do not ALTER here (was once per batch = huge slowdown).
      const queuedAt = job.queuedAt?.toISOString?.() || job.queuedAt || null;
      const startedAt = job.startedAt?.toISOString?.() || job.startedAt || null;
      const completedAt = job.completedAt?.toISOString?.() || job.completedAt || null;
      const throughput = job.throughputRps != null ? Number(job.throughputRps) : 0;
      await client.query(
        `INSERT INTO import_jobs (
          id, filename, file_size, total_rows, processed_rows, failed_rows, status,
          error_message, checkpoint_row, throughput_rps, queued_at, started_at, completed_at, cancelled
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (id) DO UPDATE SET
          filename = EXCLUDED.filename,
          file_size = EXCLUDED.file_size,
          total_rows = EXCLUDED.total_rows,
          processed_rows = EXCLUDED.processed_rows,
          failed_rows = EXCLUDED.failed_rows,
          status = EXCLUDED.status,
          error_message = EXCLUDED.error_message,
          checkpoint_row = EXCLUDED.checkpoint_row,
          throughput_rps = EXCLUDED.throughput_rps,
          queued_at = COALESCE(EXCLUDED.queued_at, import_jobs.queued_at),
          started_at = COALESCE(EXCLUDED.started_at, import_jobs.started_at),
          completed_at = EXCLUDED.completed_at,
          cancelled = EXCLUDED.cancelled`,
        [
          jobId,
          job.filename ?? null,
          job.fileSize ?? null,
          job.totalRows ?? 0,
          job.processedRows ?? 0,
          job.failedRows ?? 0,
          job.status ?? 'pending',
          job.error ?? null,
          job.checkpointRow ?? 0,
          Number.isFinite(throughput) ? throughput : 0,
          queuedAt,
          startedAt,
          completedAt,
          Boolean(job.cancelled),
        ],
      );
    });
  } catch (e) {
    logError('import', 'job db update failed', { error: e?.message });
  }
}

async function saveErrorRows(jobId, entries) {
  if (entries.length === 0) return;
  try {
    const rows = entries.map(({ rowNumber, rowData, errorMessage }) => ({
      job_id: jobId,
      row_number: rowNumber,
      row_data: rowData,
      error_message: errorMessage,
    }));
    await supabase.from('import_errors').insert(rows);
  } catch (e) {
    logError('import', 'save error rows failed', { error: e?.message });
  }
}
