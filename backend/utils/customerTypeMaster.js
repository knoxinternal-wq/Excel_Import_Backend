/**
 * customer_type_master → sales_data.business_type when Excel "TYPE OF Business" is empty.
 *
 * On import: Excel `business_type` is applied first when present; otherwise this map + default RETAILER.
 *
 * **Both sides use the same party normalizer** ({@link normalizePartyNameForBusinessTypeTables}):
 * - `customer_type_master.party_name` → map key in {@link buildCustomerTypeMasterMap} (+ M/S-stripped alias key)
 * - `sales_data.to_party_name` → same normalizer in {@link resolveBusinessType} (+ M/S strip retry)
 *
 * Value: `customer_type_master.type`. No match / empty party → `RETAILER`.
 */
import { normalizePartyNameForCustomerTypeExact } from './normalizeHeader.js';

export const DEFAULT_BUSINESS_TYPE = 'RETAILER';

const PARTY_KEYS = [
  'party_name',
  'PARTY NAME',
  'party name',
  'PARTY_NAME',
  'party',
  'to_party_name',
  'TO PARTY NAME',
  'name',
];

const TYPE_KEYS = [
  'type',
  'TYPE',
  'Types',
  'types',
  'Type',
  'business_type',
  'customer_type',
  'Type of Business',
];

/**
 * Single match key for **both** `customer_type_master.party_name` and `sales_data.to_party_name`.
 * Delegates to {@link normalizePartyNameForCustomerTypeExact} so both sides always stay aligned.
 *
 * @param {string|null|undefined} rawPartyName
 * @returns {string}
 */
export function normalizePartyNameForBusinessTypeTables(rawPartyName) {
  return normalizePartyNameForCustomerTypeExact(rawPartyName);
}

/** Normalize TYPE / business_type cell (e.g. "DISTRIBUTOR\nINTERNAL" → "DISTRIBUTOR INTERNAL"). */
export function normalizeBusinessTypeText(raw) {
  if (raw == null) return '';
  return String(raw)
    .replace(/[\u200B-\u200D\uFEFF]/g, '')
    .replace(/[\r\n]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function pickFirstNonEmptyPartyString(row, keys) {
  if (!row || typeof row !== 'object') return '';
  for (const k of keys) {
    if (!(k in row)) continue;
    const v = row[k];
    if (v == null) continue;
    const s = String(v);
    if (s.trim() === '') continue;
    return s;
  }
  return '';
}

function pickFirstNonEmptyTypeString(row, keys) {
  if (!row || typeof row !== 'object') return '';
  for (const k of keys) {
    if (!(k in row)) continue;
    const v = row[k];
    if (v == null) continue;
    const s = String(v).trim();
    if (s !== '') return s;
  }
  return '';
}

/**
 * Party string for matching (handles DB / export column name variants).
 */
export function extractMasterPartyName(row) {
  return pickFirstNonEmptyPartyString(row, PARTY_KEYS);
}

/**
 * TYPE from master row.
 */
export function extractMasterType(row) {
  const raw = pickFirstNonEmptyTypeString(row, TYPE_KEYS);
  return normalizeBusinessTypeText(raw);
}

/**
 * Build Map from `customer_type_master`: **normalize `party_name` with {@link normalizePartyNameForBusinessTypeTables}** → type.
 */
function stripMsPrefix(raw) {
  return String(raw ?? '')
    .trim()
    .replace(/^m\/s\.?\s*/i, '')
    .replace(/^ms\.?\s*/i, '')
    .replace(/^m\/s\s*/i, '')
    .trim();
}

export function buildCustomerTypeMasterMap(rows) {
  const map = new Map();
  for (const row of rows || []) {
    const rawParty = extractMasterPartyName(row);
    const typeVal = extractMasterType(row);
    if (!typeVal) continue;
    const key = normalizePartyNameForBusinessTypeTables(rawParty);
    if (!key) continue;
    map.set(key, typeVal);
    const stripped = stripMsPrefix(rawParty);
    if (stripped) {
      const k2 = normalizePartyNameForBusinessTypeTables(stripped);
      if (k2 && k2 !== key && !map.has(k2)) map.set(k2, typeVal);
    }
  }
  return map;
}

/**
 * Resolve `business_type` from **`sales_data.to_party_name`**: same normalizer as master, then Map lookup.
 */
export function resolveBusinessType(toPartyName, partyNameToTypeMap) {
  const key = normalizePartyNameForBusinessTypeTables(toPartyName);
  if (key) {
    const type = partyNameToTypeMap?.get(key);
    if (type) return type;
  }
  const stripped = stripMsPrefix(toPartyName);
  if (stripped) {
    const k2 = normalizePartyNameForBusinessTypeTables(stripped);
    if (k2 && k2 !== key) {
      const type2 = partyNameToTypeMap?.get(k2);
      if (type2) return type2;
    }
  }
  return DEFAULT_BUSINESS_TYPE;
}

export { normalizePartyNameForCustomerTypeExact };
