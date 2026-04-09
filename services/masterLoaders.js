/**
 * Single source for master-table fetches + map builds. Cached via getOrLoadMaster.
 * Logic matches previous excelProcessor / dataController implementations (same keys, same resolution).
 */
import { supabase } from '../models/supabase.js';
import { getPgPool } from '../config/database.js';
import { normalizePartyName, getPartyNameAliasKeys, normalizeAgentName, getAgentNameExactKey } from '../utils/normalizeHeader.js';
import { buildCustomerTypeMasterMap } from '../utils/customerTypeMaster.js';
import { loadSoTypeMasterMap } from '../utils/soTypeMaster.js';
import { getOrLoadMaster, invalidateMasterCache } from './masterLookupCache.js';
import { logWarn } from '../utils/logger.js';

const KEY_CUSTOMER_TYPE = 'master:customer_type_map_v2';
const KEY_SO_TYPE = 'master:so_type_map_v1';
const KEY_PARTY_GROUPING = 'master:party_grouping_map_v2';
const KEY_AGENT_NAME = 'master:agent_name_map_v1';
const KEY_REGION = 'master:region_map_v1';
const KEY_PARTY_MASTER_APP = 'master:party_master_app_v2';
const KEY_PARTY_GROUPING_SEARCH_ROWS = 'master:party_grouping_search_rows_v1';

async function loadCustomerTypeMapUncached() {
  // Support both schema styles:
  // 1) normalized columns: party_name, type
  // 2) Excel-style quoted columns: "PARTY NAME", "TYPE"
  const { data, error } = await supabase.from('customer_type_master').select('*');
  if (!error && data?.length > 0) {
    return buildCustomerTypeMasterMap(data);
  }
  if (error) {
    logWarn('masterLoaders', 'customer_type_master supabase load failed', { error: error.message });
  }
  const pool = getPgPool();
  if (pool) {
    try {
      const r = await pool.query('SELECT * FROM customer_type_master');
      if (r.rows?.length > 0) {
        return buildCustomerTypeMasterMap(r.rows);
      }
    } catch (e) {
      logWarn('masterLoaders', 'customer_type_master postgres read failed', { error: e?.message || String(e) });
    }
  }
  return buildCustomerTypeMasterMap(data || []);
}

export function getCustomerTypeMasterMap() {
  return getOrLoadMaster(KEY_CUSTOMER_TYPE, loadCustomerTypeMapUncached);
}

export function getSoTypeMasterMap() {
  return getOrLoadMaster(KEY_SO_TYPE, () => loadSoTypeMasterMap(supabase, getPgPool()));
}

/** Call after writes to Supabase SO master tables (e.g. dnj_so_master) so legacy resolveSoType sees updates. */
export function invalidateSoTypeMasterCache() {
  invalidateMasterCache(KEY_SO_TYPE);
}

function buildAgentNameMapFromRows(data) {
  const map = new Map();
  const getMasterAgentName = (row) => (
    row?.['Agent Name']
    ?? row?.agent_name
    ?? row?.agentName
    ?? null
  );
  const getMasterCombinedName = (row) => (
    row?.['Combined Name']
    ?? row?.combined_name
    ?? row?.combinedName
    ?? null
  );
  for (const row of data || []) {
    const masterAgentName = getMasterAgentName(row);
    const exactKey = getAgentNameExactKey(masterAgentName);
    const normalizedKey = normalizeAgentName(masterAgentName);
    const combinedRaw = getMasterCombinedName(row);
    const combinedName = combinedRaw != null ? String(combinedRaw).trim() : '';
    if (!combinedName) continue;
    if (exactKey && !map.has(exactKey)) map.set(exactKey, combinedName);
    if (normalizedKey && !map.has(normalizedKey)) map.set(normalizedKey, combinedName);
  }
  return map;
}

async function loadAgentNameMapUncached() {
  const { data, error } = await supabase
    .from('agent_name_master')
    .select('"Agent Name", "Combined Name"');
  if (error) {
    logWarn('masterLoaders', 'agent_name_master load failed', { error: error.message });
    const pool = getPgPool();
    if (pool) {
      try {
        const r = await pool.query('SELECT "Agent Name", "Combined Name" FROM agent_name_master');
        return buildAgentNameMapFromRows(r.rows || []);
      } catch (e) {
        logWarn('masterLoaders', 'agent_name_master postgres fallback failed', {
          error: e?.message || String(e),
        });
      }
    }
    return new Map();
  }
  const map = buildAgentNameMapFromRows(data);
  if (map.size === 0) {
    logWarn('masterLoaders', 'agent_name_master empty; agent mapping falls back to uploaded name');
  }
  return map;
}

export function getAgentNameMasterMap() {
  return getOrLoadMaster(KEY_AGENT_NAME, loadAgentNameMapUncached);
}

function trimOrNull(v) {
  if (v == null) return null;
  const s = String(v).trim();
  return s === '' ? null : s;
}

async function loadPartyGroupingMapUncached() {
  const map = new Map();
  const getMasterToPartyName = (row) => (
    row?.['TO PARTY NAME']
    ?? row?.to_party_name
    ?? row?.party_name
    ?? null
  );
  const getMasterPartyGrouped = (row) => (
    row?.['PARTY GROUPED']
    ?? row?.party_grouped
    ?? null
  );
  const getMasterPartyNameForCount = (row) => (
    row?.['PARTY NAME FOR COUNT']
    ?? row?.party_name_for_count
    ?? row?.party_name_for_cnt
    ?? null
  );
  const value = (row) => ({
    party_grouped: trimOrNull(getMasterPartyGrouped(row)),
    party_name_for_count: trimOrNull(getMasterPartyNameForCount(row)),
  });
  const addKey = (raw, row) => {
    if (raw == null || String(raw).trim() === '') return;
    const key = normalizePartyName(raw);
    if (!key) return;
    const v = value(row);
    map.set(key, v);
    for (const alt of getPartyNameAliasKeys(String(raw))) {
      if (alt && !map.has(alt)) map.set(alt, v);
    }
  };
  const ingestRows = (rows) => {
    for (const row of rows || []) {
      const tp = getMasterToPartyName(row);
      if (tp) addKey(tp, row);
    }
  };

  const pageSize = 1000;
  let from = 0;
  let supabaseFailed = false;
  for (;;) {
    const to = from + pageSize - 1;
    const { data, error } = await supabase
      .from('party_grouping_master')
      .select('to_party_name, party_grouped, party_name_for_count')
      .range(from, to);
    if (error) {
      logWarn('masterLoaders', 'party_grouping_master supabase load failed', { error: error.message });
      supabaseFailed = true;
      map.clear();
      break;
    }
    ingestRows(data);
    if (!data || data.length < pageSize) break;
    from += pageSize;
  }

  if (map.size === 0 || supabaseFailed) {
    const pool = getPgPool();
    if (pool) {
      try {
        const r = await pool.query(
          'SELECT to_party_name, party_grouped, party_name_for_count FROM party_grouping_master',
        );
        if (r.rows?.length > 0) {
          map.clear();
          ingestRows(r.rows);
        }
      } catch (e) {
        logWarn('masterLoaders', 'party_grouping_master postgres read failed', {
          error: e?.message || String(e),
        });
      }
    }
  }

  return map;
}

export function getPartyGroupingMasterMap() {
  return getOrLoadMaster(KEY_PARTY_GROUPING, loadPartyGroupingMapUncached);
}

function fillRegionMapFromRows(map, rows) {
  for (const row of rows || []) {
    const stateKey = row?.state != null ? String(row.state).trim().toUpperCase() : '';
    const regionVal = row?.region != null ? String(row.region).trim() : '';
    if (stateKey && regionVal && !map.has(stateKey)) map.set(stateKey, regionVal);
  }
}

/**
 * Load state→region from region_master (view or table), then ref_states join.
 * Supabase REST can throw `TypeError: fetch failed` (network/pooler); DATABASE_URL pool is used as fallback.
 */
async function loadRegionMasterMapUncached() {
  const map = new Map();
  const { data: rmData, error: rmError } = await supabase
    .from('region_master')
    .select('state, region');
  if (!rmError && rmData?.length > 0) {
    fillRegionMapFromRows(map, rmData);
    return map;
  }
  if (rmError) {
    logWarn('masterLoaders', 'region_master supabase unavailable', { error: rmError.message });
  }
  const pool = getPgPool();
  if (pool) {
    try {
      const r = await pool.query(
        'SELECT state, region FROM region_master WHERE state IS NOT NULL AND TRIM(state) <> \'\' AND region IS NOT NULL AND TRIM(region) <> \'\'',
      );
      if (r.rows?.length > 0) {
        fillRegionMapFromRows(map, r.rows);
        return map;
      }
    } catch (e) {
      logWarn('masterLoaders', 'region_master postgres read failed', { error: e?.message || String(e) });
    }
  }
  const { data: refData, error: refError } = await supabase
    .from('ref_states')
    .select('name, ref_regions(name)');
  if (refError) {
    logWarn('masterLoaders', 'ref_states load failed', { error: refError.message });
    return map;
  }
  for (const row of refData || []) {
    const stateKey = row?.name != null ? String(row.name).trim().toUpperCase() : '';
    const regionVal = row?.ref_regions?.name != null ? String(row.ref_regions.name).trim() : '';
    if (stateKey && regionVal && !map.has(stateKey)) map.set(stateKey, regionVal);
  }
  return map;
}

export function getRegionMasterMap() {
  return getOrLoadMaster(KEY_REGION, loadRegionMasterMapUncached);
}

/**
 * party_master_app columns vary by migration (quoted UPPER vs snake_case). PostgREST may return either.
 * @param {Record<string, unknown>} row
 * @returns {{ accountName: string, districtVal: string, pinCodeVal: string }}
 */
function pickPartyMasterFields(row) {
  if (!row || typeof row !== 'object') {
    return { accountName: '', districtVal: '', pinCodeVal: '' };
  }
  const firstNonEmpty = (keys) => {
    for (const k of keys) {
      if (row[k] == null) continue;
      const s = String(row[k]).trim();
      if (s !== '') return s;
    }
    return '';
  };
  return {
    accountName: firstNonEmpty(['ACCOUNT_NAME', 'account_name', 'Account_Name']),
    districtVal: firstNonEmpty(['DISTRICT', 'district']),
    pinCodeVal: firstNonEmpty(['PIN_CODE', 'pin_code', 'Pin_Code', 'PIN CODE']),
  };
}

function buildPartyMasterMapFromRows(rows) {
  const map = new Map();
  for (const row of rows || []) {
    const { accountName, districtVal, pinCodeVal } = pickPartyMasterFields(row);
    if (!accountName) continue;
    const district = districtVal || null;
    const pin_code = pinCodeVal || null;
    const key = normalizePartyName(accountName);
    if (!map.has(key)) map.set(key, { district, pin_code });
    for (const alt of getPartyNameAliasKeys(accountName)) {
      if (alt && !map.has(alt)) map.set(alt, { district, pin_code });
    }
  }
  return map;
}

async function loadPartyMasterMapUncached() {
  const pool = getPgPool();
  if (pool) {
    try {
      const r = await pool.query('SELECT * FROM party_master_app');
      const rowCount = r.rows?.length ?? 0;
      if (rowCount > 0) {
        const map = buildPartyMasterMapFromRows(r.rows);
        if (map.size === 0) {
          logWarn('masterLoaders', 'party_master_app: rows in DB but no ACCOUNT_NAME/account_name on rows', {
            sampleKeys: r.rows[0] ? Object.keys(r.rows[0]) : [],
            rowCount,
          });
        } else {
          return map;
        }
      }
    } catch (e) {
      logWarn('masterLoaders', 'party_master_app postgres read failed', { error: e?.message || String(e) });
    }
  }

  const map = new Map();
  const pageSize = 1000;
  let from = 0;
  for (;;) {
    const to = from + pageSize - 1;
    const { data, error } = await supabase
      .from('party_master_app')
      .select('*')
      .range(from, to);
    if (error) {
      logWarn('masterLoaders', 'party_master_app supabase load failed', { error: error.message });
      return new Map();
    }
    for (const row of data || []) {
      const { accountName, districtVal, pinCodeVal } = pickPartyMasterFields(row);
      if (!accountName) continue;
      const district = districtVal || null;
      const pin_code = pinCodeVal || null;
      const key = normalizePartyName(accountName);
      if (!map.has(key)) map.set(key, { district, pin_code });
      for (const alt of getPartyNameAliasKeys(accountName)) {
        if (alt && !map.has(alt)) map.set(alt, { district, pin_code });
      }
    }
    if (!data || data.length < pageSize) break;
    from += pageSize;
  }

  return map;
}

export function getPartyMasterAppMap() {
  return getOrLoadMaster(KEY_PARTY_MASTER_APP, loadPartyMasterMapUncached);
}

/** Flat rows for party_grouped search expansion (same data as map source, smaller payload in memory). */
async function loadPartyGroupingSearchRowsUncached() {
  const rows = [];
  const pageSize = 1000;
  let from = 0;
  for (;;) {
    const to = from + pageSize - 1;
    const { data, error } = await supabase
      .from('party_grouping_master')
      .select('to_party_name, party_grouped, party_name_for_count')
      .range(from, to);
    if (error) {
      logWarn('masterLoaders', 'party_grouping search rows load failed', { error: error.message });
      const pool = getPgPool();
      if (pool) {
        try {
          const r = await pool.query(
            'SELECT to_party_name, party_grouped, party_name_for_count FROM party_grouping_master',
          );
          return r.rows || [];
        } catch (e) {
          logWarn('masterLoaders', 'party_grouping search rows postgres fallback failed', {
            error: e?.message || String(e),
          });
        }
      }
      return [];
    }
    for (const row of data || []) rows.push(row);
    if (!data || data.length < pageSize) break;
    from += pageSize;
  }
  return rows;
}

export function getPartyGroupingSearchRows() {
  return getOrLoadMaster(KEY_PARTY_GROUPING_SEARCH_ROWS, loadPartyGroupingSearchRowsUncached);
}
