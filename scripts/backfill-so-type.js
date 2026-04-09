import fs from 'node:fs';
import path from 'node:path';
import { parseFactNumeric } from '../utils/salesFacts.js';

const MASTER_TABLES = [
  'dnj_so_master',
  'ic_so_master',
  'rf_so_master',
  'vercelli_so_master',
];

const MASTER_PAGE_SIZE = 1000;
const SALES_PAGE_SIZE = 1000;
const UPDATE_BATCH_SIZE = 1000;
const TOP_UNMATCHED_LIMIT = 20;

function loadEnvFromFile(envPath) {
  if (!fs.existsSync(envPath)) return;
  const raw = fs.readFileSync(envPath, 'utf8');
  for (const line of raw.split(/\r?\n/)) {
    const s = line.trim();
    if (!s || s.startsWith('#')) continue;
    const idx = s.indexOf('=');
    if (idx <= 0) continue;
    const key = s.slice(0, idx).trim();
    const val = s.slice(idx + 1).trim();
    if (!key || !val) continue;
    if (process.env[key] == null) process.env[key] = val;
  }
}

function normalize(name) {
  if (!name) return '';

  return name
    .toString()
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .replace(/\s*-\s*/g, '-') // normalize dash spacing
    .replace(/[\n\r\t]/g, '')
    .replace(/[^\w\s-]/g, '')
    .trim();
}

function getPartyName(row) {
  return row?.['PARTY NAME'] ?? row?.party_name ?? row?.name ?? '';
}

function getTypeOfOrder(row) {
  const raw = row?.['TYPE OF ORDER'] ?? row?.type_of_order ?? row?.so_type ?? '';
  return raw != null ? String(raw).trim() : '';
}

function getOrderDateMs(row) {
  const raw = row?.['SO ORDER DATE'] ?? row?.so_order_date ?? row?.order_date ?? null;
  if (!raw) return 0;
  const d = new Date(String(raw).trim());
  return Number.isNaN(d.getTime()) ? 0 : d.getTime();
}

async function loadAllMasterRows(supabase) {
  const allRows = [];
  const tableStats = [];

  for (const tableName of MASTER_TABLES) {
    let from = 0;
    let tableCount = 0;

    for (;;) {
      const to = from + MASTER_PAGE_SIZE - 1;
      const { data, error } = await supabase
        .from(tableName)
        .select('*')
        .range(from, to);

      if (error) throw new Error(`[${tableName}] ${error.message}`);

      const rows = (data || []).map((r) => ({ ...r, __tableName: tableName }));
      allRows.push(...rows);
      tableCount += rows.length;

      if (rows.length < MASTER_PAGE_SIZE) break;
      from += MASTER_PAGE_SIZE;
    }

    tableStats.push({ tableName, rows: tableCount });
  }

  return { allRows, tableStats };
}

function buildMasterMap(masterRows) {
  const soTypeMap = new Map();
  let duplicateConflictCount = 0;
  const duplicateConflictExamples = [];

  for (const row of masterRows) {
    const key = normalize(getPartyName(row));
    const value = getTypeOfOrder(row);
    const orderDateMs = getOrderDateMs(row);
    if (!key || !value) continue;

    if (soTypeMap.has(key)) {
      const existing = soTypeMap.get(key);
      if (existing.typeOfOrder !== value) {
        duplicateConflictCount++;
        if (duplicateConflictExamples.length < 20) {
          duplicateConflictExamples.push({
            key,
            existing: existing.typeOfOrder,
            incoming: value,
            existingTable: existing.tableName,
            incomingTable: row.__tableName,
          });
        }
      }
      if (orderDateMs >= (existing.orderDateMs ?? 0)) {
        soTypeMap.set(key, {
          typeOfOrder: value,
          orderDateMs,
          tableName: row.__tableName,
        });
      }
      continue;
    }

    soTypeMap.set(key, {
      typeOfOrder: value,
      orderDateMs,
      tableName: row.__tableName,
    });
  }

  return { soTypeMap, duplicateConflictCount, duplicateConflictExamples };
}

async function backfillSalesSoType(supabase, soTypeMap) {
  let totalSalesRowsProcessed = 0;
  let matchedCount = 0;
  let unmatchedCount = 0;
  let lastId = 0;
  const unmatchedExamples = [];

  for (;;) {
    const { data, error } = await supabase
      .from('sales_data')
      .select('id,to_party_name,so_type,sl_qty')
      .gt('id', lastId)
      .order('id', { ascending: true })
      .limit(SALES_PAGE_SIZE);

    if (error) throw new Error(`[sales_data] ${error.message}`);
    const rows = data || [];
    if (rows.length === 0) break;

    totalSalesRowsProcessed += rows.length;
    lastId = rows[rows.length - 1]?.id ?? lastId;

    const updates = [];
    for (const row of rows) {
      const lookupKey = normalize(row.to_party_name);
      const slQtyNum = parseFactNumeric(row.sl_qty, 'sl_qty');
      const soType = (slQtyNum != null && slQtyNum <= 0)
        ? 'RETURN'
        : (soTypeMap.get(lookupKey)?.typeOfOrder || null);

      if (soType) matchedCount++;
      else {
        unmatchedCount++;
        if (unmatchedExamples.length < TOP_UNMATCHED_LIMIT) {
          unmatchedExamples.push({
            id: row.id,
            to_party_name: row.to_party_name,
            normalized: lookupKey,
          });
        }
      }

      updates.push({ id: row.id, so_type: soType });
    }

    for (let i = 0; i < updates.length; i += UPDATE_BATCH_SIZE) {
      const batch = updates.slice(i, i + UPDATE_BATCH_SIZE);
      const { error: upsertError } = await supabase
        .from('sales_data')
        .upsert(batch, { onConflict: 'id' });
      if (upsertError) throw new Error(`[sales_data upsert] ${upsertError.message}`);
    }

    console.log(
      `[SO TYPE backfill] processed=${totalSalesRowsProcessed} matched=${matchedCount} unmatched=${unmatchedCount} lastId=${lastId}`,
    );
  }

  return {
    totalSalesRowsProcessed,
    matchedCount,
    unmatchedCount,
    unmatchedExamples,
  };
}

async function main() {
  const envCandidates = [
    path.resolve(process.cwd(), '.env'),
    path.resolve(process.cwd(), 'backend', '.env'),
  ];
  for (const p of envCandidates) loadEnvFromFile(p);
  const { supabase } = await import('../models/supabase.js');

  const { allRows: masterRows, tableStats } = await loadAllMasterRows(supabase);
  const { soTypeMap, duplicateConflictCount, duplicateConflictExamples } = buildMasterMap(masterRows);

  const {
    totalSalesRowsProcessed,
    matchedCount,
    unmatchedCount,
    unmatchedExamples,
  } = await backfillSalesSoType(supabase, soTypeMap);

  console.log('\n=== SO TYPE GLOBAL BACKFILL COMPLETE ===');
  console.log('master table row counts:', tableStats);
  console.log('total master records loaded:', masterRows.length);
  console.log('unique normalized master keys:', soTypeMap.size);
  console.log('duplicate conflict count:', duplicateConflictCount);
  if (duplicateConflictExamples.length > 0) {
    console.log('duplicate conflict examples (up to 20):', duplicateConflictExamples);
  }
  console.log('total sales rows processed:', totalSalesRowsProcessed);
  console.log('matched count:', matchedCount);
  console.log('unmatched count:', unmatchedCount);
  console.log('top unmatched examples (up to 20):', unmatchedExamples);
}

main().catch((e) => {
  console.error('[SO TYPE backfill] FAILED:', e);
  process.exit(1);
});

