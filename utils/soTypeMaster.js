import { normalizePartyName } from './normalizeHeader.js';
import { logWarn } from './logger.js';

export const SO_TYPE_MASTER_TABLES = [
  'dnj_so_master',
  'ic_so_master',
  'rf_so_master',
  'vercelli_so_master',
];

function normalizeTypeOfOrder(raw) {
  if (raw == null) return '';
  return String(raw)
    .replace(/[\r\n]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function parseOrderDateMs(row) {
  const raw = row?.['SO ORDER DATE'] ?? row?.so_order_date ?? row?.order_date ?? null;
  if (!raw) return 0;
  const d = new Date(String(raw).trim());
  return Number.isNaN(d.getTime()) ? 0 : d.getTime();
}

function extractPartyName(row) {
  if (!row || typeof row !== 'object') return '';
  return (
    row.party_name ??
    row['PARTY NAME'] ??
    row['party name'] ??
    row.PARTY_NAME ??
    row.name ??
    ''
  );
}

function extractTypeOfOrder(row) {
  if (!row || typeof row !== 'object') return '';
  const raw = (
    row.type_of_order ??
    row['TYPE OF ORDER'] ??
    row['type of order'] ??
    row.TYPE_OF_ORDER ??
    row.so_type ??
    ''
  );
  return normalizeTypeOfOrder(raw);
}

function addSoTypeKey(map, rawPartyName, typeOfOrder, orderDateMs = 0) {
  const key = normalizePartyName(rawPartyName);
  if (!key || !typeOfOrder) return;

  const existing = map.get(key);
  if (!existing || orderDateMs >= (existing.orderDateMs ?? 0)) {
    map.set(key, { typeOfOrder, orderDateMs });
  }
}

export function buildSoTypeMasterMap(rows) {
  const map = new Map();
  for (const row of rows || []) {
    addSoTypeKey(map, extractPartyName(row), extractTypeOfOrder(row), parseOrderDateMs(row));
  }
  return map;
}

export async function loadSoTypeMasterMap(supabase) {
  const map = new Map();
  const pageSize = 1000;
  for (const tableName of SO_TYPE_MASTER_TABLES) {
    let from = 0;
    for (;;) {
      const to = from + pageSize - 1;
      const { data, error } = await supabase.from(tableName).select('*').range(from, to);
      if (error) {
        logWarn('soTypeMaster', 'table load skipped', { tableName, error: error.message });
        break;
      }
      for (const row of data || []) {
        addSoTypeKey(map, extractPartyName(row), extractTypeOfOrder(row), parseOrderDateMs(row));
      }
      if (!data || data.length < pageSize) break;
      from += pageSize;
    }
  }
  return map;
}

export function resolveSoType(toPartyName, soTypeMap, fallback = null) {
  const key = normalizePartyName(toPartyName);
  if (!key) return fallback;
  const hit = soTypeMap?.get(key);
  return (hit?.typeOfOrder ?? fallback);
}
