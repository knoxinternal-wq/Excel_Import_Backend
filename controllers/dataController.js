import { supabase } from '../models/supabase.js';
import { getPgPool } from '../config/database.js';
import { GRAND_TOTAL_ROW_PATTERNS, MAX_SALES_ROWS } from '../config/constants.js';
import { parseFactNumeric } from '../utils/salesFacts.js';
import { normalizePartyName, getPartyNameAliasKeys, getPartyNameFilterValues, normalizeAgentName, getAgentNameExactKey } from '../utils/normalizeHeader.js';
import { resolveBusinessType } from '../utils/customerTypeMaster.js';
import { resolveSoType } from '../utils/soTypeMaster.js';
import { getOrLoadMaster, invalidateMasterCachePrefix } from '../services/masterLookupCache.js';
import {
  getCustomerTypeMasterMap,
  getSoTypeMasterMap,
  getPartyGroupingMasterMap,
  getAgentNameMasterMap,
  getRegionMasterMap,
  getPartyMasterAppMap,
  getPartyGroupingSearchRows,
} from '../services/masterLoaders.js';
import { logInfo, logError } from '../utils/logger.js';

/** Cache exact row counts so paging the Data tab does not re-run COUNT(*) every request (large tables). */
const COUNT_CACHE_TTL_MS = Number(process.env.DATA_COUNT_CACHE_TTL_MS) || 120 * 1000;
const FILTER_OPTIONS_CACHE_TTL_MS = Number(process.env.DATA_FILTER_OPTIONS_CACHE_TTL_MS) || 60 * 1000;
const DATA_PAGE_CACHE_TTL_MS = Number(process.env.DATA_PAGE_CACHE_TTL_MS) || 15_000;
const DATA_PAGE_CACHE_MAX = Number(process.env.DATA_PAGE_CACHE_MAX) || 150;
const dataPageCache = new Map();

/** Clear paginated data cache after bulk mutations (e.g. delete by date range). */
export function clearDataPageCache() {
  dataPageCache.clear();
}

/** Check if row looks like a grand total row (key columns contain "Grand Total", "Total", etc.) */
function isGrandTotalRow(row) {
  if (!row) return false;
  const cells = [
    row.branch,
    row.item_no,
    row.to_party_name,
    row.party_name_for_count,
    row.party_grouped,
    row.agent_names_correction,
    row.brand,
  ]
    .filter((v) => v != null && String(v).trim() !== '')
    .map((v) => String(v).trim().toLowerCase());
  for (const cell of cells) {
    if (!cell) continue;
    for (const p of GRAND_TOTAL_ROW_PATTERNS) {
      if (p === 'total') {
        if (cell === 'total' || cell === 'grand total' || cell.startsWith('grand total')) return true;
      } else if (cell.includes(p)) return true;
    }
  }
  return false;
}

const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/**
 * Derive FY, MONTH, MMM from bill_date when they are null (Indian FY: 1 Apr - 31 Mar).
 * Ensures FY and MONTH are visible for rows imported before derivation was added.
 * When date parsing fails, fallback: derive from numeric month (1-12) + fy (2025-26).
 */
function deriveFYMonthFromBillDate(billDate, fallbackMonth, fallbackFy) {
  let year, monthNum;

  if (billDate) {
    let d;
    if (billDate instanceof Date) {
      d = billDate;
    } else {
      const s = String(billDate).trim();
      const ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
      if (ymd) d = new Date(parseInt(ymd[1]), parseInt(ymd[2]) - 1, parseInt(ymd[3]));
      else d = new Date(s);
    }
    if (d && !isNaN(d.getTime()) && d.getFullYear() >= 2000) {
      year = d.getFullYear();
      monthNum = d.getMonth() + 1;
    }
  }

  if (year == null && typeof fallbackMonth === 'number' && fallbackMonth >= 1 && fallbackMonth <= 12 && fallbackFy) {
    const fyMatch = String(fallbackFy).match(/^(\d{4})-(\d{2})$/);
    if (fyMatch) {
      const fyStart = parseInt(fyMatch[1], 10);
      monthNum = fallbackMonth;
      year = monthNum >= 4 ? fyStart : fyStart + 1;
    }
  }

  if (year == null || monthNum == null) return { fy: null, month: null, mmm: null };
  const fyYear = monthNum >= 4 ? year : year - 1;
  const mmmLabel = MONTH_NAMES[monthNum - 1] || '';
  return {
    fy: `${fyYear}-${String(fyYear + 1).slice(-2)}`,
    month: mmmLabel ? `${mmmLabel}-${String(year).slice(-2)}` : null,
    mmm: mmmLabel ? mmmLabel.toUpperCase() : null,
  };
}

/**
 * Single-pass enrichment: apply all display enrichments in one loop.
 */
function enrichRowsSinglePass(rows, partyNameToTypeMap, soTypeMap, partyGroupingMap, agentNameMap, regionMap, partyMasterMap) {
  const result = [];
  for (const row of rows || []) {
    const resolvedBusinessType = resolveBusinessType(row.to_party_name ?? row.toPartyName, partyNameToTypeMap);
    const currentBusinessType = resolvedBusinessType ?? row.business_type ?? null;
    const branchForRule = row?.branch != null ? String(row.branch).trim().toUpperCase() : '';
    const businessTypeForRule = currentBusinessType != null ? String(currentBusinessType).trim().toUpperCase() : '';
    const businessType = (branchForRule === 'ITALIAN CHANNEL' && businessTypeForRule === 'RETAILER')
      ? 'DISTRIBUTOR'
      : currentBusinessType;
    const rawSlQty = parseFactNumeric(row?.sl_qty, 'sl_qty');
    const storedSoType = row?.so_type != null && String(row.so_type).trim() !== '' ? String(row.so_type).trim() : null;
    const normalizedBrand = row?.brand != null ? String(row.brand).trim().toUpperCase() : '';
    let soType;
    if (rawSlQty != null && rawSlQty <= 0) {
      soType = 'RETURN';
    } else if (normalizedBrand === 'RISHAB FABRICS') {
      soType = 'NO SCHEME';
    } else {
      soType = resolveSoType(row.to_party_name ?? row.toPartyName, soTypeMap, storedSoType);
    }
    const stateKey = row?.state != null ? String(row.state).trim().toUpperCase() : '';
    const region = stateKey && regionMap?.get(stateKey) ? regionMap.get(stateKey) : row.region;
    const toPartyName = (row.to_party_name ?? row.toPartyName ?? '').toString().trim();
    let district = row.district;
    let pinCode = row.pin_code;
    if (toPartyName && partyMasterMap) {
      const dKey = normalizePartyName(toPartyName);
      let match = partyMasterMap.get(dKey);
      if (!match) {
        for (const alt of getPartyNameAliasKeys(toPartyName)) {
          match = partyMasterMap.get(alt);
          if (match) break;
        }
      }
      if (match?.district) district = match.district;
      if (match?.pin_code) pinCode = match.pin_code;
    }
    let party_grouped = row.party_grouped;
    let party_name_for_count = row.party_name_for_count ?? (toPartyName || null);
    if (toPartyName) {
      const key = normalizePartyName(toPartyName);
      let master = partyGroupingMap?.get(key);
      if (!master) {
        for (const alt of getPartyNameAliasKeys(toPartyName)) {
          master = partyGroupingMap?.get(alt);
          if (master) break;
        }
      }
      if (master) {
        party_name_for_count = master.party_name_for_count ?? toPartyName;
        party_grouped = master.party_grouped ?? toPartyName;
      } else {
        party_grouped = toPartyName;
      }
    }
    const excelNameRaw = row.agent_name != null ? String(row.agent_name) : '';
    const excelNameTrimmed = excelNameRaw.trim();
    const excelName = excelNameTrimmed || null;
    const exactKey = getAgentNameExactKey(row.agent_name);
    const normalizedKey = normalizeAgentName(row.agent_name);
    const combinedName = (exactKey || normalizedKey) ? (agentNameMap?.get(exactKey) ?? agentNameMap?.get(normalizedKey)) : null;
    const displayName = combinedName || excelName;
    const dateSrc = row.bill_date ?? row.sale_order_date;
    let derived = deriveFYMonthFromBillDate(dateSrc);
    if (!derived?.fy) {
      const numMonth = Number(row.month);
      const fyStr = String(row.fy || '').trim();
      const fyMatch = fyStr.match(/^(\d{4})-(\d{2})$/);
      if (fyMatch && numMonth >= 1 && numMonth <= 12) {
        const startYear = parseInt(fyMatch[1], 10);
        const year = numMonth >= 4 ? startYear : startYear + 1;
        const mmmLabel = MONTH_NAMES[numMonth - 1] || '';
        derived = { fy: fyStr, month: `${mmmLabel}-${String(year).slice(-2)}`, mmm: mmmLabel.toUpperCase() };
      }
    }
    const itemNo = row.item_no != null ? String(row.item_no).trim() : '';
    const shadeName = row.shade_name != null ? String(row.shade_name).trim() : '';
    const parts = [itemNo, shadeName].filter((s) => s !== '');
    const itemWithShade = parts.length > 0 ? parts.join(' ') : (row.item_with_shade ?? null);
    const itemSubCat = (row.item_sub_cat != null && String(row.item_sub_cat).trim() !== '') ? row.item_sub_cat : (row.scheme != null && String(row.scheme).trim() !== '' ? row.scheme : row.item_sub_cat);
    const brandUpper = row?.brand != null ? String(row.brand).trim().toUpperCase() : '';
    const branchUpper = row?.branch != null ? String(row.branch).trim().toUpperCase() : '';
    const branch = (brandUpper === 'RARE WOOL' && branchUpper === 'DON AND JULIO') ? 'RARE WOOL' : row.branch;
    const enriched = {
      ...row,
      branch,
      business_type: businessType,
      region: region ?? row.region,
      district: district ?? row.district,
      pin_code: pinCode ?? row.pin_code,
      party_grouped,
      party_name_for_count,
      agent_name_final: displayName,
      agent_names_correction: displayName,
      fy: derived?.fy ?? row.fy,
      month: derived?.month ?? row.month,
      mmm: derived?.mmm ?? row.mmm,
      item_with_shade: itemWithShade,
      item_sub_cat: itemSubCat,
      so_type: soType,
    };
    if (!isGrandTotalRow(enriched)) result.push(enriched);
  }
  return result;
}

function canSkipRuntimeEnrichment(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return true;
  return rows.every((r) => (
    r
    && r.business_type != null
    && r.so_type != null
    && r.region != null
    && r.party_grouped != null
    && r.party_name_for_count != null
    && r.agent_name_final != null
    && r.agent_names_correction != null
  ));
}

export async function getData(req, res) {
  try {
    let page = Math.max(1, parseInt(req.query.page) || 1);
    const limit = Math.min(300, Math.max(10, parseInt(req.query.limit) || 100));
    // Set includeTotal=0 to skip COUNT (faster but no exact page count). Data UI defaults to totals for full pagination.
    const includeTotal = String(req.query.includeTotal ?? '1') === '1';
    const maxPageByCap = Math.max(1, Math.ceil(MAX_SALES_ROWS / limit));
    page = Math.min(page, maxPageByCap);
    const search = req.query.search || '';
    const state = req.query.state || '';
    const cursorId = Number(req.query.cursorId) || null;
    const sortBy = req.query.sortBy || 'id';
    const sortOrder = req.query.sortOrder === 'asc';
    const useKeysetPaging = String(req.query.paging || '').toLowerCase() === 'keyset';

    const validSortColumns = [
      'id', 'bill_date', 'bill_no', 'item_no', 'agent_name', 'branch', 'net_amount', 'goods_type',
      'created_at', 'fy', 'month', 'region', 'state', 'district', 'city', 'gross_amount', 'sale_order_date',
    ];
    const orderColumn = validSortColumns.includes(sortBy) ? sortBy : 'id';

    // Select only what the UI needs (smaller payload = faster).
    const DATA_SELECT = [
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
    ].join(',');

    const applyFilters = (q) => {
      // Search: Bill No. and Party Grouped only (party_grouped via sales_data + party_grouping_master)
      const trimmedSearch = (search || '').trim();
      if (trimmedSearch.length >= 2) {
        const term = `%${trimmedSearch}%`;
        const pattern = `*${trimmedSearch}*`;
        const orParts = [`bill_no.ilike.${pattern}`, `party_grouped.ilike.${pattern}`];

        // Include party_grouping_master: rows whose party_grouped matches → get to_party_name for sales filter
        // NOTE: this is potentially expensive; keep it but it’s filtered by search.
        // (still async in the main flow below)
        q.__needsPartyMaster = { term, orParts, trimmedSearch };
      }
      if (state) q = q.eq('state', state);
      return q;
    };

    const MAX_PARTY_NAME_MATCHES = 120;
    const appendPartyMasterSearch = async (orParts, trimmedSearch) => {
      const masterRows = await getPartyGroupingSearchRows();

      const toPartyValues = new Set();
      for (const r of masterRows || []) {
        const partyGrouped = r?.['PARTY GROUPED'] ?? r?.party_grouped ?? null;
        if (!partyGrouped || !String(partyGrouped).toLowerCase().includes(String(trimmedSearch || '').toLowerCase())) {
          continue;
        }
        const matchVal = r?.['TO PARTY NAME'] ?? r?.to_party_name ?? r?.party_name;
        if (!matchVal) continue;

        toPartyValues.add(matchVal);
        for (const v of getPartyNameFilterValues(matchVal)) {
          if (v) toPartyValues.add(v);
        }

        // Keep OR filter size bounded; oversized in(...) queries can fail.
        if (toPartyValues.size >= MAX_PARTY_NAME_MATCHES) break;
      }

      if (toPartyValues.size > 0) {
        const inList = [...toPartyValues]
          .slice(0, MAX_PARTY_NAME_MATCHES)
          .map((v) => `"${String(v).replace(/"/g, '""')}"`)
          .join(',');
        orParts.push(`to_party_name.in.(${inList})`);
      }
      return orParts;
    };

    const countKey = `sales_data_count|search=${(search || '').trim()}|state=${state || ''}`;

    const buildDataCacheKey = (pageNum) => JSON.stringify({
      page: pageNum,
      limit,
      includeTotal,
      state,
      search: String(search || '').trim().toLowerCase(),
      sortBy: orderColumn,
      sortOrder: sortOrder ? 'asc' : 'desc',
      cursorId: cursorId || 0,
      paging: useKeysetPaging ? 'keyset' : 'offset',
    });
    const cacheItem = dataPageCache.get(buildDataCacheKey(page));
    if (cacheItem && (Date.now() - cacheItem.ts) < DATA_PAGE_CACHE_TTL_MS) {
      res.set('Cache-Control', 'private, max-age=0, must-revalidate');
      res.set('X-Data-Cache', 'HIT');
      return res.json(cacheItem.payload);
    }

    const runCount = () => getOrLoadMaster(countKey, async () => {
      const useEstimatedCount = String(process.env.DATA_USE_ESTIMATED_COUNT ?? '1') === '1';
      const countMode = useEstimatedCount ? 'planned' : 'exact';
      let countQuery = applyFilters(supabase.from('sales_data').select('id', { count: countMode, head: true }));
      if (countQuery.__needsPartyMaster) {
        const { orParts, trimmedSearch } = countQuery.__needsPartyMaster;
        delete countQuery.__needsPartyMaster;
        if (trimmedSearch.length >= 3) {
          await appendPartyMasterSearch(orParts, trimmedSearch);
        }
        countQuery = countQuery.or(orParts.join(','));
      }
      const r = await countQuery;
      if (r.error) {
        return null;
      }
      return r.count ?? 0;
    }, COUNT_CACHE_TTL_MS);

    /**
     * Fetch one page of rows (offset or keyset). Used alone or in parallel with COUNT on first page.
     */
    const fetchRowsForPage = async (pageNum) => {
      let query = applyFilters(supabase.from('sales_data').select(DATA_SELECT));
      if (query.__needsPartyMaster) {
        const { orParts, trimmedSearch } = query.__needsPartyMaster;
        delete query.__needsPartyMaster;
        if (trimmedSearch.length >= 3) {
          await appendPartyMasterSearch(orParts, trimmedSearch);
        }
        query = query.or(orParts.join(','));
      }
      query = query.order(orderColumn, { ascending: sortOrder });
      if (useKeysetPaging && orderColumn === 'id' && cursorId != null) {
        query = sortOrder ? query.gt('id', cursorId) : query.lt('id', cursorId);
        query = query.limit(limit);
      } else {
        const from = (pageNum - 1) * limit;
        const to = from + limit - 1;
        query = query.range(from, to);
      }
      return query;
    };

    let total = null;
    let rows = [];
    /** First Data tab load: COUNT was fully sequential before row query — huge delay on Lakh-scale tables. */
    const parallelCountAndRows = includeTotal && page === 1 && !useKeysetPaging;

    if (parallelCountAndRows) {
      const [totalResult, rowResult] = await Promise.all([runCount(), fetchRowsForPage(1)]);
      total = totalResult;
      page = 1;
      rows = rowResult.data ?? [];
      if (rowResult.error) throw new Error(rowResult.error.message);
    } else {
      if (includeTotal) {
        total = await runCount();
      }
      let totalPagesForClamp = maxPageByCap;
      if (includeTotal && total != null) {
        totalPagesForClamp = Math.max(1, Math.ceil(total / limit));
      }
      page = Math.min(page, totalPagesForClamp);

      const rowResult = await fetchRowsForPage(page);
      rows = rowResult.data ?? [];
      if (rowResult.error) throw new Error(rowResult.error.message);
    }

    let enrichedRows = rows || [];
    if (!canSkipRuntimeEnrichment(enrichedRows)) {
      const [partyNameToTypeMap, soTypeMap, partyGroupingMap, agentNameMap, regionMap, partyMasterMap] = await Promise.all([
        getCustomerTypeMasterMap(),
        getSoTypeMasterMap(),
        getPartyGroupingMasterMap(),
        getAgentNameMasterMap(),
        getRegionMasterMap(),
        getPartyMasterAppMap(),
      ]);
      enrichedRows = enrichRowsSinglePass(enrichedRows, partyNameToTypeMap, soTypeMap, partyGroupingMap, agentNameMap, regionMap, partyMasterMap);
    } else {
      enrichedRows = enrichedRows.filter((row) => !isGrandTotalRow(row));
    }

    const fallbackTotal = ((page - 1) * limit) + (rows?.length || 0);
    const resolvedTotal = includeTotal
      ? (total == null ? fallbackTotal : total)
      : null;
    const resolvedTotalPages = includeTotal
      ? (total == null
        ? Math.max(1, rows?.length === limit ? page + 1 : page)
        : (Math.ceil(total / limit) || 1))
      : Math.max(1, (rows?.length || 0) < limit ? page : page + 1);

    const payload = {
      data: enrichedRows,
      pagination: {
        page,
        limit,
        total: resolvedTotal,
        totalPages: resolvedTotalPages,
        nextCursor: rows && rows.length ? rows[rows.length - 1]?.id ?? null : null,
      },
    };
    dataPageCache.set(buildDataCacheKey(page), { ts: Date.now(), payload });
    while (dataPageCache.size > DATA_PAGE_CACHE_MAX) {
      const k = dataPageCache.keys().next().value;
      dataPageCache.delete(k);
    }
    res.set('Cache-Control', 'private, max-age=0, must-revalidate');
    res.set('X-Data-Cache', 'MISS');
    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

export async function getStates(req, res) {
  try {
    const pool = getPgPool();
    if (!pool) throw new Error('DATABASE_URL is not configured');
    const { rows } = await pool.query(
      `SELECT DISTINCT state
       FROM sales_data
       WHERE state IS NOT NULL
         AND btrim(state) <> ''
       ORDER BY state ASC`,
    );
    const states = (rows || [])
      .map((r) => String(r?.state ?? '').trim())
      .filter(Boolean);
    res.json(states);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

const ISO_DATE_RE = /^(\d{4})-(\d{2})-(\d{2})$/;

/**
 * @param {unknown} v
 * @returns {string | null} YYYY-MM-DD or null
 */
function parseIsoDateOnly(v) {
  if (v == null) return null;
  const s = String(v).trim();
  if (!ISO_DATE_RE.test(s)) return null;
  const [, y, mo, d] = s.match(ISO_DATE_RE);
  const yi = parseInt(y, 10);
  const mi = parseInt(mo, 10);
  const di = parseInt(d, 10);
  const dt = new Date(Date.UTC(yi, mi - 1, di));
  if (dt.getUTCFullYear() !== yi || dt.getUTCMonth() !== mi - 1 || dt.getUTCDate() !== di) return null;
  return s;
}

/**
 * @returns {{ ok: true, fromDate: string, toDate: string } | { ok: false, error: string }}
 */
function validateDeleteRangeBody(body) {
  const fromDate = parseIsoDateOnly(body?.fromDate);
  const toDate = parseIsoDateOnly(body?.toDate);
  if (!fromDate || !toDate) {
    return { ok: false, error: 'fromDate and toDate are required (YYYY-MM-DD).' };
  }
  if (fromDate > toDate) {
    return { ok: false, error: 'fromDate must be on or before toDate.' };
  }
  return { ok: true, fromDate, toDate };
}

/**
 * POST /api/data/delete-range/preview
 */
export async function previewDeleteByDateRange(req, res) {
  try {
    const parsed = validateDeleteRangeBody(req.body);
    if (!parsed.ok) {
      return res.status(400).json({ success: false, error: parsed.error });
    }
    const pool = getPgPool();
    if (!pool) {
      return res.status(503).json({ success: false, error: 'DATABASE_URL is not configured.' });
    }
    const { rows } = await pool.query(
      `SELECT COUNT(*)::bigint AS c
       FROM sales_data
       WHERE bill_date IS NOT NULL AND bill_date BETWEEN $1::date AND $2::date`,
      [parsed.fromDate, parsed.toDate],
    );
    const count = rows[0]?.c != null ? Number(rows[0].c) : 0;
    logInfo('data', 'delete-range preview', {
      fromDate: parsed.fromDate,
      toDate: parsed.toDate,
      count,
    });
    return res.json({ success: true, count });
  } catch (err) {
    logError('data', 'delete-range preview failed', { message: err?.message });
    return res.status(500).json({ success: false, error: err?.message || 'Preview failed.' });
  }
}

/**
 * DELETE /api/data/delete-range
 * Single indexed DELETE; transaction with optional statement_timeout disabled for large batches.
 */
export async function deleteByDateRange(req, res) {
  const parsed = validateDeleteRangeBody(req.body);
  if (!parsed.ok) {
    return res.status(400).json({ success: false, error: parsed.error });
  }
  const pool = getPgPool();
  if (!pool) {
    return res.status(503).json({ success: false, error: 'DATABASE_URL is not configured.' });
  }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const noTimeout = String(process.env.DATA_DELETE_RANGE_NO_STATEMENT_TIMEOUT ?? '1').trim() === '1';
    if (noTimeout) {
      await client.query('SET LOCAL statement_timeout = 0');
    }
    const del = await client.query(
      `DELETE FROM sales_data
       WHERE bill_date IS NOT NULL AND bill_date BETWEEN $1::date AND $2::date`,
      [parsed.fromDate, parsed.toDate],
    );
    await client.query('COMMIT');
    const deletedRows = Number(del.rowCount ?? 0);
    logInfo('data', 'delete-range committed', {
      fromDate: parsed.fromDate,
      toDate: parsed.toDate,
      deletedRows,
    });
    clearDataPageCache();
    invalidateMasterCachePrefix('sales_data_count');
    invalidateMasterCachePrefix('sales_data_filter_options');
    return res.json({ success: true, deletedRows });
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      /* ignore */
    }
    logError('data', 'delete-range failed', { message: err?.message, stack: err?.stack });
    return res.status(500).json({ success: false, error: err?.message || 'Delete failed.' });
  } finally {
    client.release();
  }
}

export async function getFilterOptions(req, res) {
  try {
    const key = 'sales_data_filter_options_v1';
    const payload = await getOrLoadMaster(key, async () => {
      const pool = getPgPool();
      if (!pool) throw new Error('DATABASE_URL is not configured');
      const [statesRes, partyRes, bizRes] = await Promise.all([
        pool.query(
          `SELECT DISTINCT state
           FROM sales_data
           WHERE state IS NOT NULL AND btrim(state) <> ''
           ORDER BY state ASC`,
        ),
        pool.query(
          `SELECT DISTINCT party_grouped
           FROM sales_data
           WHERE party_grouped IS NOT NULL AND btrim(party_grouped) <> ''
           ORDER BY party_grouped ASC`,
        ),
        pool.query(
          `SELECT DISTINCT business_type
           FROM sales_data
           WHERE business_type IS NOT NULL AND btrim(business_type) <> ''
           ORDER BY business_type ASC`,
        ),
      ]);
      const states = (statesRes.rows || []).map((r) => String(r.state || '').trim()).filter(Boolean);
      const partyGroups = (partyRes.rows || []).map((r) => String(r.party_grouped || '').trim()).filter(Boolean);
      const businessTypes = (bizRes.rows || []).map((r) => String(r.business_type || '').trim()).filter(Boolean);
      return { states, partyGroups, businessTypes };
    }, FILTER_OPTIONS_CACHE_TTL_MS);
    res.set('Cache-Control', 'private, max-age=30');
    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
