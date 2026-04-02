/**
 * customer_type_master → sales_data.business_type ("TYPE OF Business").
 * Match: sales_data.to_party_name ↔ master party column; value ↔ master TYPE column.
 */
import { normalizePartyName, getPartyNameAliasKeys } from './normalizeHeader.js';

export const DEFAULT_BUSINESS_TYPE = 'RETAILER';

/** Normalize TYPE cell (e.g. "DISTRIBUTOR\nINTERNAL" → "DISTRIBUTOR INTERNAL"). */
function normalizeTypeValue(raw) {
  if (raw == null) return '';
  return String(raw)
    .replace(/[\r\n]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Read party name from a master row (handles DB / Supabase column name variants).
 */
export function extractMasterPartyName(row) {
  if (!row || typeof row !== 'object') return '';
  const v =
    row.party_name ??
    row['PARTY NAME'] ??
    row['party name'] ??
    row.PARTY_NAME ??
    row.party ??
    row.to_party_name ??
    row['TO PARTY NAME'] ??
    row.name ??
    '';
  return v;
}

/**
 * Read TYPE from a master row (matches TYPE column in customer_type_master).
 */
export function extractMasterType(row) {
  if (!row || typeof row !== 'object') return '';
  const v =
    row.type ??
    row['TYPE'] ??
    row.Type ??
    row.business_type ??
    row.customer_type ??
    row['Type of Business'] ??
    '';
  return normalizeTypeValue(v);
}

/**
 * Build Map: normalized party key → TYPE string.
 * Registers primary key (normalizePartyName) and M/S alias keys like party_grouping.
 */
export function buildCustomerTypeMasterMap(rows) {
  const map = new Map();
  for (const row of rows || []) {
    const rawParty = extractMasterPartyName(row);
    const typeVal = extractMasterType(row);
    if (!typeVal) continue;
    const primary = normalizePartyName(rawParty);
    if (primary) {
      map.set(primary, typeVal);
      for (const alt of getPartyNameAliasKeys(rawParty)) {
        if (alt && !map.has(alt)) map.set(alt, typeVal);
      }
    }
  }
  return map;
}

/**
 * Resolve business_type from to_party_name using the master map.
 * No match or empty party → RETAILER.
 */
export function resolveBusinessType(toPartyName, partyNameToTypeMap) {
  const key = normalizePartyName(toPartyName);
  if (!key) return DEFAULT_BUSINESS_TYPE;
  let type = partyNameToTypeMap?.get(key);
  if (!type) {
    for (const alt of getPartyNameAliasKeys(toPartyName)) {
      type = partyNameToTypeMap?.get(alt);
      if (type) break;
    }
  }
  return type ?? DEFAULT_BUSINESS_TYPE;
}
