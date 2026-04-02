import fs from 'fs';
import ExcelJS from 'exceljs';
import { getPgPool } from '../config/database.js';
import { normalizeHeader } from '../utils/normalizeHeader.js';
import { invalidateSoMasterCache } from '../services/soMasterLoader.js';
import { invalidateSoTypeMasterCache } from '../services/masterLoaders.js';
import { logError, logInfo, logWarn } from '../utils/logger.js';

/**
 * Admin SO master upload targets the legacy SO-type masters based on brand.
 * This keeps resolveSoType fast (no runtime joins on sales import).
 */
const BRAND_TO_LEGACY_TABLE = {
  'DON AND JULIO': 'dnj_so_master',
  'ITALIAN CHANNEL': 'ic_so_master',
  'RISHAB FABRICS': 'rf_so_master',
  'VERCELLI': 'vercelli_so_master',
};

const HISTORY_TABLE = 'admin_so_master_upload_history';

const LEGACY_TABLE_TO_EDIT_HISTORY = {
  dnj_so_master: 'admin_dnj_so_master_edit_history',
  ic_so_master: 'admin_ic_so_master_edit_history',
  rf_so_master: 'admin_rf_so_master_edit_history',
  vercelli_so_master: 'admin_vercelli_so_master_edit_history',
};

const MASTER_TABLE_CONFIG = {
  dnj_so_master: {
    label: 'DNJ SO Master',
    syncSalesSoType: true,
  },
  ic_so_master: {
    label: 'IC SO Master',
    syncSalesSoType: true,
  },
  rf_so_master: {
    label: 'RF SO Master',
    syncSalesSoType: true,
  },
  vercelli_so_master: {
    label: 'Vercelli SO Master',
    syncSalesSoType: true,
  },
  agent_name_master: {
    label: 'Agent Name Master',
    syncSalesSoType: false,
  },
  customer_type_master: {
    label: 'Customer Type Master',
    syncSalesSoType: false,
  },
  party_grouping_master: {
    label: 'Party Grouping Master',
    syncSalesSoType: false,
  },
  region_master: {
    label: 'Region Master',
    syncSalesSoType: false,
  },
};

const MASTER_ROW_EDIT_HISTORY_EVENT_TABLE = 'admin_master_table_row_edit_history_event';
const MASTER_ROW_EDIT_HISTORY_BEFORE_TABLE = 'admin_master_table_row_edit_history_before';
const MASTER_ROW_EDIT_HISTORY_AFTER_TABLE = 'admin_master_table_row_edit_history_after';

function quoteIdent(value) {
  return `"${String(value).replace(/"/g, '""')}"`;
}

async function getTableColumns(client, table) {
  const { rows } = await client.query(
    `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = $1
      ORDER BY ordinal_position ASC
    `,
    [table],
  );
  return (rows || []).map((r) => r.column_name);
}

async function getTableType(client, table) {
  const { rows } = await client.query(
    `
      SELECT table_type
      FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = $1
      LIMIT 1
    `,
    [table],
  );
  return rows?.[0]?.table_type || null; // BASE TABLE / VIEW
}

function sanitizeHistoryRowSnapshot(row) {
  if (!row || typeof row !== 'object') return null;
  const out = {};
  for (const [key, value] of Object.entries(row)) {
    if (['uuid', 'id', 'created_at', '__row_id'].includes(key)) continue;
    out[key] = value;
  }
  return Object.keys(out).length ? out : null;
}

async function loadHistoryRowSnapshots(client, historyRows) {
  const uniqueTargets = new Map();
  for (const row of historyRows || []) {
    const table = String(row?.master_table || '').trim();
    const rowUuid = String(row?.row_uuid || '').trim();
    if (!MASTER_TABLE_CONFIG[table] || !rowUuid) continue;
    uniqueTargets.set(`${table}::${rowUuid}`, { table, rowUuid });
  }

  const snapshots = new Map();
  for (const { table, rowUuid } of uniqueTargets.values()) {
    // eslint-disable-next-line no-await-in-loop
    const columns = await getTableColumns(client, table);
    if (!columns.length) continue;
    const idColumn = columns.includes('uuid') ? 'uuid' : (columns.includes('id') ? 'id' : null);
    const rowWhereExpr = idColumn ? `${quoteIdent(idColumn)}::text` : 'ctid::text';

    // eslint-disable-next-line no-await-in-loop
    const rowRes = await client.query(
      `
        SELECT *, ${idColumn ? `${quoteIdent(idColumn)}::text` : 'ctid::text'} AS __row_id
        FROM ${quoteIdent(table)}
        WHERE ${rowWhereExpr} = $1
        LIMIT 1
      `,
      [rowUuid],
    );
    const snapshot = sanitizeHistoryRowSnapshot(rowRes.rows?.[0] || null);
    if (snapshot) snapshots.set(`${table}::${rowUuid}`, snapshot);
  }

  return snapshots;
}

async function syncSalesDataFromMasterEdit({ client, table, prev, next, changedColumns }) {
  if (!changedColumns?.size) return;

  if (['dnj_so_master', 'ic_so_master', 'rf_so_master', 'vercelli_so_master'].includes(table)) {
    const prevParty = prev.party_name != null ? String(prev.party_name).trim().toUpperCase() : '';
    const prevBrand = prev.brand != null ? String(prev.brand).trim().toUpperCase() : '';
    const nextParty = next.party_name != null ? String(next.party_name).trim().toUpperCase() : '';
    const nextBrand = next.brand != null ? String(next.brand).trim().toUpperCase() : '';
    const nextSoTypeRaw = next.type_of_order != null ? String(next.type_of_order).trim() : '';
    const nextSoType = nextSoTypeRaw || 'UNKNOWN';

    if (prevParty && prevBrand) {
      await client.query(
        `
          UPDATE sales_data
          SET so_type = $1
          WHERE UPPER(TRIM(to_party_name)) = $2
            AND UPPER(TRIM(brand)) = $3
            AND (so_type IS NULL OR UPPER(so_type) <> 'RETURN')
        `,
        [nextSoType, prevParty, prevBrand],
      );
    }

    if ((prevParty !== nextParty || prevBrand !== nextBrand) && nextParty && nextBrand) {
      await client.query(
        `
          UPDATE sales_data
          SET so_type = $1
          WHERE UPPER(TRIM(to_party_name)) = $2
            AND UPPER(TRIM(brand)) = $3
            AND (so_type IS NULL OR UPPER(so_type) <> 'RETURN')
        `,
        [nextSoType, nextParty, nextBrand],
      );
    }
    return;
  }

  if (table === 'agent_name_master') {
    const prevAgent = prev['Agent Name'] != null ? String(prev['Agent Name']).trim() : '';
    const nextAgent = next['Agent Name'] != null ? String(next['Agent Name']).trim() : '';
    const prevCombined = prev['Combined Name'] != null ? String(prev['Combined Name']).trim() : '';
    const nextCombined = next['Combined Name'] != null ? String(next['Combined Name']).trim() : '';

    if (prevAgent && nextAgent && prevAgent !== nextAgent) {
      await client.query(
        `UPDATE sales_data SET agent_name = $1 WHERE UPPER(TRIM(agent_name)) = UPPER(TRIM($2))`,
        [nextAgent, prevAgent],
      );
    }

    const agentForMapping = nextAgent || prevAgent;
    if (agentForMapping) {
      await client.query(
        `
          UPDATE sales_data
          SET agent_names_correction = $1,
              agent_name_final = $1
          WHERE UPPER(TRIM(agent_name)) = UPPER(TRIM($2))
        `,
        [nextCombined || null, agentForMapping],
      );
    } else if (prevCombined !== nextCombined) {
      await client.query(
        `
          UPDATE sales_data
          SET agent_names_correction = $1,
              agent_name_final = $1
          WHERE UPPER(TRIM(agent_names_correction)) = UPPER(TRIM($2))
             OR UPPER(TRIM(agent_name_final)) = UPPER(TRIM($2))
        `,
        [nextCombined || null, prevCombined || ''],
      );
    }
    return;
  }

  if (table === 'customer_type_master') {
    const prevParty = prev.party_name != null ? String(prev.party_name).trim() : '';
    const nextParty = next.party_name != null ? String(next.party_name).trim() : '';
    const prevType = prev.type != null ? String(prev.type).trim() : '';
    const nextType = next.type != null ? String(next.type).trim() : '';

    if (prevParty) {
      await client.query(
        `
          UPDATE sales_data
          SET business_type = $1
          WHERE UPPER(TRIM(to_party_name)) = UPPER(TRIM($2))
            AND (
              business_type IS NULL
              OR UPPER(TRIM(business_type)) = UPPER(TRIM($3))
              OR UPPER(TRIM($3)) = ''
            )
        `,
        [nextType || null, prevParty, prevType || ''],
      );
    }
    if (nextParty && nextParty !== prevParty) {
      await client.query(
        `
          UPDATE sales_data
          SET business_type = $1
          WHERE UPPER(TRIM(to_party_name)) = UPPER(TRIM($2))
        `,
        [nextType || null, nextParty],
      );
    }
    return;
  }

  if (table === 'party_grouping_master') {
    const prevParty = prev.to_party_name != null ? String(prev.to_party_name).trim() : '';
    const nextParty = next.to_party_name != null ? String(next.to_party_name).trim() : '';
    const nextGrouped = next.party_grouped != null ? String(next.party_grouped).trim() : null;
    const nextCount = next.party_name_for_count != null ? String(next.party_name_for_count).trim() : null;

    if (prevParty) {
      await client.query(
        `
          UPDATE sales_data
          SET party_grouped = $1,
              party_name_for_count = $2
          WHERE UPPER(TRIM(to_party_name)) = UPPER(TRIM($3))
        `,
        [nextGrouped, nextCount, prevParty],
      );
    }
    if (nextParty && nextParty !== prevParty) {
      await client.query(
        `
          UPDATE sales_data
          SET party_grouped = $1,
              party_name_for_count = $2
          WHERE UPPER(TRIM(to_party_name)) = UPPER(TRIM($3))
        `,
        [nextGrouped, nextCount, nextParty],
      );
    }
    return;
  }

  if (table === 'region_master') {
    const prevState = prev.state != null ? String(prev.state).trim() : '';
    const nextState = next.state != null ? String(next.state).trim() : '';
    const nextRegion = next.region != null ? String(next.region).trim() : null;

    if (prevState) {
      await client.query(
        `
          UPDATE sales_data
          SET region = $1
          WHERE UPPER(TRIM(state)) = UPPER(TRIM($2))
        `,
        [nextRegion, prevState],
      );
    }
    if (nextState && nextState !== prevState) {
      await client.query(
        `
          UPDATE sales_data
          SET region = $1
          WHERE UPPER(TRIM(state)) = UPPER(TRIM($2))
        `,
        [nextRegion, nextState],
      );
    }
  }
}

/**
 * Canonicalize brand input from UI/excel text to the supported legacy keys.
 * Note: we still accept historical variants (D&J/DNJ/RARE WOOL) by mapping them to DON AND JULIO.
 */
function canonicalizeBrand(brandRaw) {
  const b = String(brandRaw ?? '')
    .replace(/\s+/g, ' ')
    .trim()
    .toUpperCase();

  // D&J family: normalize everything to "DON AND JULIO"
  if ([
    'DON AND JULIO',
    'DON & JULIO',
    'DON&JULIO',
    'DON &JULIO',
    'D&J',
    'DNJ',
    'RARE WOOL',
  ].includes(b)) {
    return 'DON AND JULIO';
  }
  if (b === 'ITALIAN CHANNEL') return 'ITALIAN CHANNEL';
  if (b === 'IC') return 'ITALIAN CHANNEL';
  // Raw uploads/sales sometimes use short forms like RF.
  if (b === 'RISHAB FABRICS' || b === 'RF' || b === 'RISHAB') return 'RISHAB FABRICS';
  if (b === 'VERCELLI') return 'VERCELLI';
  return b; // unsupported brand: still written to so_master, but not synced to legacy masters
}

async function syncRowsToLegacyTable({ rows, tableName, legacyBrand }) {
  if (!rows?.length || !tableName) return;

  const chunkSize = 200;
  const pool = getPgPool();
  if (!pool) return;

  // Legacy tables have snake_case columns:
  //   party_name, so_agent_name, branch, company_name, so_order_no, so_order_date, type_of_order, brand, fy
  // Unique constraint: (party_name, brand, fy)
  if (!/^(dnj_so_master|ic_so_master|rf_so_master|vercelli_so_master)$/.test(tableName)) {
    throw new Error(`Invalid legacy table: ${tableName}`);
  }

  const baseSql = `
    INSERT INTO ${tableName} (
      party_name,
      so_agent_name,
      branch,
      company_name,
      so_order_no,
      so_order_date,
      type_of_order,
      brand,
      fy
    )
    VALUES %VALUES%
    ON CONFLICT (party_name, brand, fy)
    DO UPDATE SET
      so_agent_name = COALESCE(EXCLUDED.so_agent_name, ${tableName}.so_agent_name),
      branch = COALESCE(EXCLUDED.branch, ${tableName}.branch),
      company_name = COALESCE(EXCLUDED.company_name, ${tableName}.company_name),
      so_order_no = COALESCE(EXCLUDED.so_order_no, ${tableName}.so_order_no),
      so_order_date = COALESCE(EXCLUDED.so_order_date, ${tableName}.so_order_date),
      type_of_order = COALESCE(EXCLUDED.type_of_order, ${tableName}.type_of_order)
  `;

  const brand = String(legacyBrand ?? '').trim().toUpperCase();
  const fy = String(rows[0]?.fy ?? '').trim();
  if (!fy) {
    // If fy isn't passed in the row objects, fallback to a bulk insert with the same fy not possible.
    throw new Error('Missing fy for legacy table sync');
  }

  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);

    const params = [];
    const valuesSql = chunk.map((r, idx) => {
      const p = idx * 9;
      params.push(
        String(r.party_name).trim().toUpperCase(),
        r.so_agent_name != null ? String(r.so_agent_name).trim().toUpperCase() : null,
        r.branch != null ? String(r.branch).trim().toUpperCase() : null,
        r.company_name != null ? String(r.company_name).trim().toUpperCase() : null,
        r.so_order_no != null ? String(r.so_order_no).trim() : null,
        r.so_order_date != null ? String(r.so_order_date).trim() : null,
        r.type_of_order != null ? String(r.type_of_order).trim() : null,
        brand,
        fy,
      );
      return `($${p + 1}, $${p + 2}, $${p + 3}, $${p + 4}, $${p + 5}, $${p + 6}, $${p + 7}, $${p + 8}, $${p + 9})`;
    }).join(', ');

    const sql = baseSql.replace('%VALUES%', valuesSql);
    await pool.query(sql, params);
  }

  invalidateSoTypeMasterCache();
}

/**
 * Find required SO master columns in the first rows of the sheet.
 * @param {import('exceljs').Worksheet} sheet
 * @returns {{
 *  headerRow: number,
 *  partyCol: number,
 *  soAgentCol: number,
 *  branchCol: number,
 *  companyCol: number,
 *  soOrderNoCol: number,
 *  soOrderDateCol: number,
 *  typeCol: number
 * } | null}
 */
function findSoMasterColumns(sheet) {
  const maxScan = Math.min(30, sheet.rowCount || 0);
  const alias = {
    party: new Set([
      normalizeHeader('PARTY NAME'),
      normalizeHeader('PARTY'),
      normalizeHeader('TO PARTY NAME'),
      normalizeHeader('PARTYNAME'),
    ]),
    soAgent: new Set([
      normalizeHeader('SO AGENT NAME'),
      normalizeHeader('SO AGENT'),
      normalizeHeader('AGENT NAME'),
      normalizeHeader('AGENT'),
    ]),
    branch: new Set([
      normalizeHeader('BRANCH'),
      normalizeHeader('BRANCH NAME'),
    ]),
    company: new Set([
      normalizeHeader('COMPANY NAME'),
      normalizeHeader('COMPANY'),
      normalizeHeader('COMPANYNAME'),
    ]),
    soOrderNo: new Set([
      normalizeHeader('SO ORDER NO.'),
      normalizeHeader('SO ORDER NO'),
      normalizeHeader('SO ORDER NUMBER'),
      normalizeHeader('SO NO'),
      normalizeHeader('ORDER NO'),
      normalizeHeader('ORDER NUMBER'),
    ]),
    soOrderDate: new Set([
      normalizeHeader('SO ORDER DATE'),
      normalizeHeader('SO DATE'),
      normalizeHeader('ORDER DATE'),
      normalizeHeader('SOORDERDATE'),
    ]),
    type: new Set([
      normalizeHeader('TYPE OF ORDER'),
      normalizeHeader('ORDER TYPE'),
      normalizeHeader('TYPE'),
      normalizeHeader('SO TYPE'),
      normalizeHeader('TYPE OF ORDER '),
    ]),
  };

  const includesToken = (key, token) => key.includes(normalizeHeader(token));

  for (let r = 1; r <= maxScan; r++) {
    const row = sheet.getRow(r);
    let partyCol = 0;
    let soAgentCol = 0;
    let branchCol = 0;
    let companyCol = 0;
    let soOrderNoCol = 0;
    let soOrderDateCol = 0;
    let typeCol = 0;
    row.eachCell({ includeEmpty: false }, (cell, colNumber) => {
      const c = row.getCell(colNumber);
      const key = normalizeHeader(cellToString(c));
      if (alias.party.has(key) || includesToken(key, 'PARTY NAME')) partyCol = colNumber;
      if (alias.soAgent.has(key) || (includesToken(key, 'AGENT') && includesToken(key, 'SO'))) soAgentCol = colNumber;
      if (alias.branch.has(key)) branchCol = colNumber;
      if (alias.company.has(key) || includesToken(key, 'COMPANY NAME')) companyCol = colNumber;
      if (alias.soOrderNo.has(key) || (includesToken(key, 'ORDER') && includesToken(key, 'NO'))) soOrderNoCol = colNumber;
      if (alias.soOrderDate.has(key) || (includesToken(key, 'ORDER') && includesToken(key, 'DATE'))) soOrderDateCol = colNumber;
      if (alias.type.has(key) || (includesToken(key, 'TYPE') && includesToken(key, 'ORDER'))) typeCol = colNumber;
    });
    // Required: PARTY NAME + TYPE OF ORDER.
    // Others are optional and can remain 0 (treated as blank during import).
    if (partyCol > 0 && typeCol > 0) {
      return {
        headerRow: r,
        partyCol,
        soAgentCol,
        branchCol,
        companyCol,
        soOrderNoCol,
        soOrderDateCol,
        typeCol,
      };
    }
  }
  return null;
}

function getCellSafe(row, colNumber) {
  if (!colNumber || colNumber <= 0) return null;
  return row.getCell(colNumber);
}

function collectHeaderCandidates(sheet, maxRows = 5) {
  const out = [];
  const scan = Math.min(maxRows, sheet.rowCount || 0);
  for (let r = 1; r <= scan; r += 1) {
    const row = sheet.getRow(r);
    const cells = [];
    row.eachCell({ includeEmpty: false }, (cell, colNumber) => {
      const v = normalizeHeader(cellToString(row.getCell(colNumber)));
      if (v) cells.push(v);
    });
    if (cells.length) out.push({ row: r, headers: cells });
  }
  return out;
}

function cellToString(cell) {
  if (cell == null) return '';
  const v = cell.value;
  if (v == null) return '';
  if (typeof v === 'object' && v !== null && v.richText && Array.isArray(v.richText)) {
    return v.richText.map((x) => x.text || '').join('').trim();
  }
  if (typeof v === 'object' && v !== null && 'text' in v) return String(v.text ?? '').trim();
  if (typeof v === 'object' && v !== null && 'result' in v && v.result != null) return String(v.result).trim();
  return String(v).trim();
}

function excelSerialToISODate(serial) {
  // Excel serial date: days since 1899-12-30 (with the historical 1900 leap-year bug).
  // Works well for common date serials.
  const s = Number(serial);
  if (!Number.isFinite(s)) return null;
  const days = Math.floor(s);
  // JS Date: 1970-01-01 offset uses ms.
  const epoch = Date.UTC(1899, 11, 30);
  const ms = days * 24 * 60 * 60 * 1000;
  const d = new Date(epoch + ms);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

function cellToDateString(cell) {
  if (cell == null) return null;
  const v = cell.value;
  if (v == null || v === '') return null;

  if (v instanceof Date) {
    if (Number.isNaN(v.getTime())) return null;
    return v.toISOString().slice(0, 10);
  }

  if (typeof v === 'number' && Number.isFinite(v)) {
    const iso = excelSerialToISODate(v);
    if (iso) return iso;
  }

  const s = String(v).trim();
  if (!s) return null;

  const ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (ymd) return `${ymd[1]}-${ymd[2]}-${ymd[3]}`;

  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
}

/** Idempotent DDL so uploads work before manual schema migration. */
async function ensureSoMasterTable(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS so_master (
      id SERIAL PRIMARY KEY,
      party_name TEXT NOT NULL,
      type_of_order TEXT NOT NULL,
      brand TEXT,
      fy TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);
  await client.query(`
    ALTER TABLE so_master ADD COLUMN IF NOT EXISTS brand TEXT;
    ALTER TABLE so_master ADD COLUMN IF NOT EXISTS fy TEXT;
  `);
  await client.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'unique_so') THEN
        ALTER TABLE so_master ADD CONSTRAINT unique_so UNIQUE (party_name, brand, fy);
      END IF;
    END $$;
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_so_master_lookup ON so_master (party_name, brand, fy);
  `);
}

async function ensureUploadHistoryTable(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${HISTORY_TABLE} (
      id BIGSERIAL PRIMARY KEY,
      filename TEXT,
      brand TEXT,
      fy TEXT,
      status TEXT NOT NULL,
      inserted_rows INTEGER DEFAULT 0,
      error_message TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_admin_so_history_created_at
    ON ${HISTORY_TABLE} (created_at DESC);
  `);
}

async function ensureEditHistoryTables(client) {
  for (const historyTable of Object.values(LEGACY_TABLE_TO_EDIT_HISTORY)) {
    // eslint-disable-next-line no-await-in-loop
    await client.query(`
      CREATE TABLE IF NOT EXISTS ${historyTable} (
        id BIGSERIAL PRIMARY KEY,
        party_name TEXT NOT NULL,
        master_table TEXT,
        brand TEXT NOT NULL,
        fy TEXT NOT NULL,
        previous_type_of_order TEXT,
        new_type_of_order TEXT,
        edited_by TEXT,
        created_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);
    // eslint-disable-next-line no-await-in-loop
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_${historyTable}_created_at
      ON ${historyTable} (created_at DESC);
    `);

    await client.query(`
      ALTER TABLE ${historyTable}
      ADD COLUMN IF NOT EXISTS master_table TEXT;
    `);
  }
}

async function ensureMasterRowEditHistoryTable(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${MASTER_ROW_EDIT_HISTORY_EVENT_TABLE} (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      master_table TEXT NOT NULL,
      row_uuid TEXT NOT NULL,
      column_name TEXT NOT NULL,
      edited_by TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_admin_master_row_edit_history_created_at
    ON ${MASTER_ROW_EDIT_HISTORY_EVENT_TABLE} (created_at DESC);
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${MASTER_ROW_EDIT_HISTORY_BEFORE_TABLE} (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      event_id UUID NOT NULL REFERENCES ${MASTER_ROW_EDIT_HISTORY_EVENT_TABLE}(id) ON DELETE CASCADE,
      previous_value TEXT
    );
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${MASTER_ROW_EDIT_HISTORY_AFTER_TABLE} (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      event_id UUID NOT NULL REFERENCES ${MASTER_ROW_EDIT_HISTORY_EVENT_TABLE}(id) ON DELETE CASCADE,
      new_value TEXT
    );
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_admin_master_row_edit_before_event_id
    ON ${MASTER_ROW_EDIT_HISTORY_BEFORE_TABLE} (event_id);
  `);
  await client.query(`
    CREATE INDEX IF NOT EXISTS idx_admin_master_row_edit_after_event_id
    ON ${MASTER_ROW_EDIT_HISTORY_AFTER_TABLE} (event_id);
  `);
}

async function insertEditHistory({
  client,
  historyTable,
  masterTable,
  partyName,
  brand,
  fy,
  previousTypeOfOrder,
  newTypeOfOrder,
  editedBy,
}) {
  await client.query(
    `
      INSERT INTO ${historyTable} (
        party_name,
        master_table,
        brand,
        fy,
        previous_type_of_order,
        new_type_of_order,
        edited_by
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7)
    `,
    [
      partyName,
      masterTable,
      brand,
      fy,
      previousTypeOfOrder ?? null,
      newTypeOfOrder ?? null,
      editedBy ?? null,
    ],
  );
}

async function insertUploadHistory({ client, filename, brand, fy, status, insertedRows = 0, errorMessage = null }) {
  if (!client) return;
  await client.query(
    `
      INSERT INTO ${HISTORY_TABLE} (filename, brand, fy, status, inserted_rows, error_message)
      VALUES ($1, $2, $3, $4, $5, $6)
    `,
    [filename, brand, fy, status, insertedRows, errorMessage],
  );
}

/**
 * GET /api/admin/so-master-preview?brand=&fy=
 * Returns a small preview of legacy master rows used for so_type mapping.
 */
export async function previewSoMaster(req, res) {
  const pool = getPgPool();
  if (!pool) {
    return res.status(500).json({ success: false, error: 'Database not configured.' });
  }

  try {
    const brandRaw = req.query?.brand != null ? String(req.query.brand).trim() : '';
    const fyRaw = req.query?.fy != null ? String(req.query.fy).trim() : '';
    const limitRaw = req.query?.limit != null ? Number(req.query.limit) : 20;
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(200, Math.floor(limitRaw))) : 20;

    if (!brandRaw) return res.status(400).json({ success: false, error: 'brand is required.' });
    if (!fyRaw) return res.status(400).json({ success: false, error: 'fy is required.' });

    const brand = canonicalizeBrand(brandRaw);
    const tableName = BRAND_TO_LEGACY_TABLE[brand] || null;
    if (!tableName) {
      return res.status(400).json({ success: false, error: `Unsupported brand: ${brandRaw}` });
    }

    const { rows } = await pool.query(`
      SELECT party_name,
             so_agent_name,
             branch,
             company_name,
             so_order_no,
             so_order_date,
             type_of_order,
             brand,
             fy,
             COUNT(*) OVER() AS total
      FROM ${tableName}
      WHERE brand = $1 AND fy = $2
      ORDER BY party_name ASC
      LIMIT $3
    `, [brand, fyRaw, limit]);

    const total = rows?.[0]?.total != null ? Number(rows[0].total) : 0;
    const previewRows = (rows || []).map((r) => ({
      party_name: r.party_name,
      so_agent_name: r.so_agent_name,
      branch: r.branch,
      company_name: r.company_name,
      so_order_no: r.so_order_no,
      so_order_date: r.so_order_date,
      type_of_order: r.type_of_order,
    }));

    return res.json({
      success: true,
      brand,
      fy: fyRaw,
      table: tableName,
      total,
      rows: previewRows,
    });
  } catch (e) {
    logError('admin', 'previewSoMaster failed', { message: e?.message || String(e) });
    return res.status(500).json({
      success: false,
      error: e?.message || 'Preview failed.',
    });
  }
}

/**
 * GET /api/admin/so-master-history?limit=20
 */
export async function soMasterHistory(req, res) {
  const pool = getPgPool();
  if (!pool) return res.status(500).json({ success: false, error: 'Database not configured.' });

  try {
    const limitRaw = req.query?.limit != null ? Number(req.query.limit) : 20;
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(50, Math.floor(limitRaw))) : 20;

    const { rows } = await pool.query(
      `
        SELECT filename,
               brand,
               fy,
               status,
               inserted_rows,
               error_message,
               created_at
        FROM ${HISTORY_TABLE}
        ORDER BY created_at DESC
        LIMIT $1
      `,
      [limit],
    );

    return res.json({
      success: true,
      rows: (rows || []).map((r) => ({
        filename: r.filename,
        brand: r.brand,
        fy: r.fy,
        status: r.status,
        insertedRows: r.inserted_rows,
        errorMessage: r.error_message,
        createdAt: r.created_at,
      })),
    });
  } catch (e) {
    logError('admin', 'soMasterHistory failed', { message: e?.message || String(e) });
    return res.status(500).json({ success: false, error: e?.message || 'History failed.' });
  }
}

/**
 * GET /api/admin/so-master-edit-history?brand=...&fy=...&limit=20
 */
export async function soMasterEditHistory(req, res) {
  const pool = getPgPool();
  if (!pool) return res.status(500).json({ success: false, error: 'Database not configured.' });

  try {
    const brandRaw = req.query?.brand != null ? String(req.query.brand).trim() : '';
    const fyRaw = req.query?.fy != null ? String(req.query.fy).trim() : '';
    const limitRaw = req.query?.limit != null ? Number(req.query.limit) : 20;

    if (!brandRaw) return res.status(400).json({ success: false, error: 'brand is required.' });
    if (!fyRaw) return res.status(400).json({ success: false, error: 'fy is required.' });

    const brand = canonicalizeBrand(brandRaw);
    const tableName = BRAND_TO_LEGACY_TABLE[brand] || null;
    const historyTable = tableName ? LEGACY_TABLE_TO_EDIT_HISTORY[tableName] : null;
    if (!historyTable) return res.status(400).json({ success: false, error: `Unsupported brand: ${brandRaw}` });

    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(100, Math.floor(limitRaw))) : 20;

    const client = await pool.connect();
    try {
      await ensureEditHistoryTables(client);

      const { rows } = await client.query(
        `
          SELECT party_name,
                 master_table,
                 previous_type_of_order,
                 new_type_of_order,
                 edited_by,
                 created_at
          FROM ${historyTable}
          WHERE brand = $1 AND fy = $2
          ORDER BY created_at DESC
          LIMIT $3
        `,
        [brand, fyRaw, limit],
      );

      return res.json({
        success: true,
        brand,
        fy: fyRaw,
        rows: (rows || []).map((r) => ({
          party_name: r.party_name,
          master_table: r.master_table,
          previous_type_of_order: r.previous_type_of_order,
          new_type_of_order: r.new_type_of_order,
          edited_by: r.edited_by,
          created_at: r.created_at,
        })),
      });
    } finally {
      client.release();
    }
  } catch (e) {
    logError('admin', 'soMasterEditHistory failed', { message: e?.message || String(e) });
    return res.status(500).json({ success: false, error: e?.message || 'Edit history failed.' });
  }
}

/**
 * GET /api/admin/master-table-options
 */
export async function masterTableOptions(req, res) {
  const rows = Object.entries(MASTER_TABLE_CONFIG).map(([table, cfg]) => ({
    table,
    label: cfg.label,
  }));
  return res.json({ success: true, rows });
}

/**
 * GET /api/admin/master-table-preview?table=dnj_so_master&limit=200
 */
export async function masterTablePreview(req, res) {
  const pool = getPgPool();
  if (!pool) return res.status(500).json({ success: false, error: 'Database not configured.' });

  try {
    const table = req.query?.table != null ? String(req.query.table).trim() : '';
    const q = req.query?.q != null ? String(req.query.q).trim() : '';
    const limitRaw = req.query?.limit != null ? Number(req.query.limit) : 200;
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(500, Math.floor(limitRaw))) : 200;
    const cfg = MASTER_TABLE_CONFIG[table];
    if (!cfg) return res.status(400).json({ success: false, error: 'Unsupported master table.' });

    const client = await pool.connect();
    try {
      const columns = await getTableColumns(client, table);
      if (!columns.length) return res.status(404).json({ success: false, error: 'Table not found.' });

      const idColumn = columns.includes('uuid') ? 'uuid' : (columns.includes('id') ? 'id' : null);
      const rowIdExpr = idColumn ? `${quoteIdent(idColumn)}::text` : 'ctid::text';

      const orderBy = columns.includes('created_at')
        ? `${quoteIdent('created_at')} DESC NULLS LAST`
        : (columns.includes('party_name') ? `${quoteIdent('party_name')} ASC NULLS LAST` : rowIdExpr);

      const searchableColumns = columns.filter((c) => !['created_at', 'id', 'uuid'].includes(c));
      const searchWhere = q && searchableColumns.length
        ? `WHERE (${searchableColumns.map((c, i) => `CAST(${quoteIdent(c)} AS TEXT) ILIKE $${i + 2}`).join(' OR ')})`
        : '';
      const params = [limit];
      if (q && searchableColumns.length) {
        for (let i = 0; i < searchableColumns.length; i += 1) params.push(`%${q}%`);
      }

      const { rows } = await client.query(
        `
          SELECT *, ${rowIdExpr} AS __row_id
          FROM ${quoteIdent(table)}
          ${searchWhere}
          ORDER BY ${orderBy}
          LIMIT $1
        `,
        params,
      );

      const editableColumns = columns.filter((c) => !['created_at', 'id', 'uuid'].includes(c));

      return res.json({
        success: true,
        table,
        q,
        editableColumns,
        rowIdColumn: idColumn || 'ctid',
        rows: rows || [],
      });
    } finally {
      client.release();
    }
  } catch (e) {
    logError('admin', 'masterTablePreview failed', { message: e?.message || String(e) });
    return res.status(500).json({ success: false, error: e?.message || 'Preview failed.' });
  }
}

/**
 * GET /api/admin/master-table-edit-history?table=dnj_so_master&limit=100
 */
export async function masterTableEditHistory(req, res) {
  const pool = getPgPool();
  if (!pool) return res.status(500).json({ success: false, error: 'Database not configured.' });

  try {
    const table = req.query?.table != null ? String(req.query.table).trim() : '';
    const limitRaw = req.query?.limit != null ? Number(req.query.limit) : 100;
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(300, Math.floor(limitRaw))) : 100;
    const cfg = table ? MASTER_TABLE_CONFIG[table] : null;
    if (table && !cfg) return res.status(400).json({ success: false, error: 'Unsupported master table.' });

    const client = await pool.connect();
    try {
      await ensureMasterRowEditHistoryTable(client);
      const whereSql = table ? 'WHERE e.master_table = $1' : '';
      const params = table ? [table, limit] : [limit];
      const limitPlaceholder = table ? '$2' : '$1';
      const { rows } = await client.query(
        `
          SELECT e.master_table,
                 e.row_uuid,
                 e.column_name,
                 b.previous_value,
                 a.new_value,
                 e.edited_by,
                 e.created_at
          FROM ${MASTER_ROW_EDIT_HISTORY_EVENT_TABLE} e
          LEFT JOIN ${MASTER_ROW_EDIT_HISTORY_BEFORE_TABLE} b ON b.event_id = e.id
          LEFT JOIN ${MASTER_ROW_EDIT_HISTORY_AFTER_TABLE} a ON a.event_id = e.id
          ${whereSql}
          ORDER BY e.created_at DESC
          LIMIT ${limitPlaceholder}
        `,
        params,
      );
      const snapshots = await loadHistoryRowSnapshots(client, rows || []);
      return res.json({
        success: true,
        table: table || null,
        rows: (rows || []).map((row) => ({
          ...row,
          row_data: snapshots.get(`${row.master_table}::${row.row_uuid}`) || null,
        })),
      });
    } finally {
      client.release();
    }
  } catch (e) {
    logError('admin', 'masterTableEditHistory failed', { message: e?.message || String(e) });
    return res.status(500).json({ success: false, error: e?.message || 'Edit history failed.' });
  }
}

/**
 * POST /api/admin/edit-master-table-row
 * body: { table, row_uuid, updates: { ... }, edited_by? }
 */
export async function editMasterTableRow(req, res) {
  const pool = getPgPool();
  if (!pool) return res.status(500).json({ success: false, error: 'Database not configured.' });

  try {
    const table = req.body?.table != null ? String(req.body.table).trim() : '';
    const rowId = req.body?.row_id != null ? String(req.body.row_id).trim() : '';
    const updatesRaw = req.body?.updates && typeof req.body.updates === 'object' ? req.body.updates : null;
    const editedBy = req.body?.edited_by != null ? String(req.body.edited_by).trim() : 'unknown';

    const cfg = MASTER_TABLE_CONFIG[table];
    if (!cfg) return res.status(400).json({ success: false, error: 'Unsupported master table.' });
    if (!rowId) return res.status(400).json({ success: false, error: 'row_id is required.' });
    if (!updatesRaw) return res.status(400).json({ success: false, error: 'updates object is required.' });

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await ensureMasterRowEditHistoryTable(client);
      const tableType = await getTableType(client, table);
      if (tableType && tableType !== 'BASE TABLE') {
        await client.query('ROLLBACK');
        return res.status(400).json({
          success: false,
          error: `Table ${table} is not directly editable (${tableType}).`,
        });
      }
      const columns = await getTableColumns(client, table);
      if (!columns.length) {
        await client.query('ROLLBACK');
        return res.status(404).json({ success: false, error: 'Table not found.' });
      }

      const idColumn = columns.includes('uuid') ? 'uuid' : (columns.includes('id') ? 'id' : null);
      const rowWhereExpr = idColumn ? `${quoteIdent(idColumn)}::text` : 'ctid::text';
      const nonEditable = new Set([idColumn, 'created_at', '__row_id']);
      const updates = {};
      for (const [k, v] of Object.entries(updatesRaw)) {
        if (!columns.includes(k) || nonEditable.has(k)) continue;
        if (k === 'so_order_date') {
          updates[k] = v == null || String(v).trim() === '' ? null : String(v).slice(0, 10);
        } else if (k === 'sno') {
          const n = Number(v);
          updates[k] = Number.isFinite(n) ? Math.floor(n) : null;
        } else if (v == null) {
          updates[k] = null;
        } else {
          updates[k] = String(v).trim();
        }
      }
      const entries = Object.entries(updates);
      if (entries.length === 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({ success: false, error: 'No editable fields provided.' });
      }

      const prevRes = await client.query(
        `SELECT *, ${idColumn ? `${quoteIdent(idColumn)}::text` : 'ctid::text'} AS __row_id
         FROM ${quoteIdent(table)}
         WHERE ${rowWhereExpr} = $1
         LIMIT 1`,
        [rowId],
      );
      if (!prevRes.rows?.length) {
        await client.query('ROLLBACK');
        return res.status(404).json({ success: false, error: 'Row not found.' });
      }
      const prev = prevRes.rows[0];

      const setSql = entries.map(([k], idx) => `${quoteIdent(k)} = $${idx + 1}`).join(', ');
      const params = entries.map(([, v]) => v);
      const rowWherePlaceholder = `$${entries.length + 1}`;
      params.push(rowId);
      const updatedRes = await client.query(
        `UPDATE ${quoteIdent(table)}
         SET ${setSql}
         WHERE ${rowWhereExpr} = ${rowWherePlaceholder}
         RETURNING *, ${idColumn ? `${quoteIdent(idColumn)}::text` : 'ctid::text'} AS __row_id`,
        params,
      );
      const next = updatedRes.rows[0];
      const changedColumns = new Set();

      if (['dnj_so_master', 'ic_so_master', 'rf_so_master', 'vercelli_so_master'].includes(table)) {
        const siblingAssignments = entries.map(([k], idx) => `${quoteIdent(k)} = $${idx + 1}`).join(', ');
        if (siblingAssignments) {
          const siblingParams = entries.map(([, v]) => v);
          siblingParams.push(String(prev.party_name ?? '').trim().toUpperCase());
          siblingParams.push(String(prev.brand ?? '').trim().toUpperCase());
          siblingParams.push(String(next.__row_id ?? '').trim());
          await client.query(
            `
              UPDATE ${quoteIdent(table)}
              SET ${siblingAssignments}
              WHERE UPPER(TRIM(party_name)) = $${entries.length + 1}
                AND UPPER(TRIM(brand)) = $${entries.length + 2}
                AND ${rowWhereExpr} <> $${entries.length + 3}
            `,
            siblingParams,
          );
        }
      }

      for (const [col] of entries) {
        const prevVal = prev[col] == null ? null : String(prev[col]);
        const nextVal = next[col] == null ? null : String(next[col]);
        if (prevVal === nextVal) continue;
        changedColumns.add(col);
        // eslint-disable-next-line no-await-in-loop
        const eventRes = await client.query(
          `
            INSERT INTO ${MASTER_ROW_EDIT_HISTORY_EVENT_TABLE}
              (master_table, row_uuid, column_name, edited_by)
            VALUES ($1,$2,$3,$4)
            RETURNING id
          `,
          [table, next.__row_id, col, editedBy || 'unknown'],
        );
        const eventId = eventRes.rows?.[0]?.id;
        if (eventId != null) {
          // eslint-disable-next-line no-await-in-loop
          await client.query(
            `
              INSERT INTO ${MASTER_ROW_EDIT_HISTORY_BEFORE_TABLE}
                (event_id, previous_value)
              VALUES ($1,$2)
            `,
            [eventId, prevVal],
          );
          // eslint-disable-next-line no-await-in-loop
          await client.query(
            `
              INSERT INTO ${MASTER_ROW_EDIT_HISTORY_AFTER_TABLE}
                (event_id, new_value)
              VALUES ($1,$2)
            `,
            [eventId, nextVal],
          );
        }
      }

      await syncSalesDataFromMasterEdit({ client, table, prev, next, changedColumns });

      await client.query('COMMIT');
      invalidateSoTypeMasterCache();
      invalidateSoMasterCache();
      return res.json({ success: true, row: next });
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  } catch (e) {
    logError('admin', 'editMasterTableRow failed', { message: e?.message || String(e) });
    return res.status(500).json({ success: false, error: e?.message || 'Row update failed.' });
  }
}

/**
 * POST /api/admin/edit-so-master-row
 * body: { brand, fy, party_name, type_of_order }
 */
export async function editSoMasterRow(req, res) {
  const pool = getPgPool();
  if (!pool) {
    return res.status(500).json({ success: false, error: 'Database not configured.' });
  }

  try {
    const body = req.body || {};
    const brandRaw = body?.brand != null ? String(body.brand).trim() : '';
    const fyRaw = body?.fy != null ? String(body.fy).trim() : '';
    const partyNameRaw = body?.party_name != null ? String(body.party_name).trim() : '';
    const typeOfOrderRaw = body?.type_of_order != null ? String(body.type_of_order).trim() : '';
    const editedBy = body?.edited_by != null ? String(body.edited_by).trim() : '';

    if (!brandRaw) return res.status(400).json({ success: false, error: 'brand is required.' });
    if (!fyRaw) return res.status(400).json({ success: false, error: 'fy is required.' });
    if (!partyNameRaw) return res.status(400).json({ success: false, error: 'party_name is required.' });
    // type_of_order can be blank; still allow updating (record history and update sales_data accordingly).

    const brand = canonicalizeBrand(brandRaw);
    const tableName = BRAND_TO_LEGACY_TABLE[brand] || null;
    if (!tableName) return res.status(400).json({ success: false, error: `Unsupported brand: ${brandRaw}` });

    const party_name = partyNameRaw.toUpperCase();
    const type_of_order = typeOfOrderRaw;
    const fy = fyRaw;

    const historyTable = LEGACY_TABLE_TO_EDIT_HISTORY[tableName];
    if (!historyTable) return res.status(500).json({ success: false, error: 'Edit history table missing.' });

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await ensureEditHistoryTables(client);

      // Load previous value for history + to update only the rows that currently map to the old value.
      const prevRes = await client.query(
        `SELECT type_of_order
         FROM ${tableName}
         WHERE party_name = $1 AND brand = $2 AND fy = $3
         LIMIT 1`,
        [party_name, brand, fy],
      );

      if (!prevRes.rows?.length) {
        await client.query('ROLLBACK');
        return res.status(404).json({
          success: false,
          error: 'Master row not found for this party/brand/fy.',
        });
      }

      const previousTypeOfOrder = prevRes.rows[0].type_of_order ?? null;

      await client.query(
        `UPDATE ${tableName}
         SET type_of_order = $1
         WHERE party_name = $2 AND brand = $3`,
        [type_of_order, party_name, brand],
      );

      // Keep so_master in sync too (used by older code paths / debug).
      try {
        await client.query(
          `UPDATE so_master
           SET type_of_order = $1
           WHERE party_name = $2 AND brand = $3`,
          [type_of_order, party_name, brand],
        );
      } catch {
        /* ignore if table differs in older deployments */
      }

      await insertEditHistory({
        client,
        historyTable,
        masterTable: tableName,
        partyName: party_name,
        brand,
        fy,
        previousTypeOfOrder,
        newTypeOfOrder: type_of_order,
        editedBy: editedBy || 'unknown',
      });

      // Reflect in sales_data so pivot/data tab shows updated SO type.
      // Don't override RETURN rows (they are derived from quantity logic during import).
      // If master type_of_order is blank, align with import fallback by setting sales_data.so_type = 'UNKNOWN'.
      const nextSoType = type_of_order && String(type_of_order).trim() !== '' ? type_of_order : 'UNKNOWN';
      await client.query(
        `
          UPDATE sales_data
          SET so_type = $1
          WHERE UPPER(TRIM(to_party_name)) = $2
            AND UPPER(TRIM(brand)) = $3
            AND (so_type IS NULL OR UPPER(so_type) <> 'RETURN')
        `,
        [nextSoType, party_name, brand],
      );

      await client.query('COMMIT');

      invalidateSoTypeMasterCache();
      invalidateSoMasterCache();

      return res.json({
        success: true,
        updated: 1,
        previousTypeOfOrder,
        newTypeOfOrder: type_of_order,
      });
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  } catch (e) {
    logError('admin', 'editSoMasterRow failed', { message: e?.message || String(e) });
    return res.status(500).json({ success: false, error: e?.message || 'Edit failed.' });
  }
}

/**
 * POST /api/admin/import-so-master
 * multipart: file, brand, fy
 */
export async function importSoMaster(req, res) {
  const pool = getPgPool();
  if (!pool) {
    return res.status(500).json({ success: false, error: 'Database not configured.' });
  }

  try {
    if (!req.file) {
      return res.status(400).json({ success: false, error: 'No file uploaded.' });
    }

    const brandRaw = req.body?.brand != null ? String(req.body.brand).trim() : '';
    const fyRaw = req.body?.fy != null ? String(req.body.fy).trim() : '';
    const originalFilename = req.file?.originalname || req.file?.filename || 'so-master.xlsx';

    if (!brandRaw) {
      try {
        fs.unlinkSync(req.file.path);
      } catch {
        /* ignore */
      }
      return res.status(400).json({ success: false, error: 'Brand is required.' });
    }
    if (!fyRaw) {
      try {
        fs.unlinkSync(req.file.path);
      } catch {
        /* ignore */
      }
      return res.status(400).json({ success: false, error: 'FY is required.' });
    }

    const brand = canonicalizeBrand(brandRaw);
    const legacyTable = BRAND_TO_LEGACY_TABLE[brand] || null;
    const fy = fyRaw;

    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(req.file.path);

    const sheet = workbook.worksheets[0];
    if (!sheet) {
      try {
        fs.unlinkSync(req.file.path);
      } catch {
        /* ignore */
      }
      return res.status(400).json({ success: false, error: 'Workbook has no sheets.' });
    }

    const cols = findSoMasterColumns(sheet);
    if (!cols) {
      const detected = collectHeaderCandidates(sheet, 5);
      try {
        fs.unlinkSync(req.file.path);
      } catch {
        /* ignore */
      }
      return res.status(400).json({
        success: false,
        error: 'Invalid file: expected columns PARTY NAME and TYPE OF ORDER. Other SO columns are optional.',
        detectedHeaders: detected,
      });
    }

    const valid = [];
    sheet.eachRow((row, rowNumber) => {
      if (rowNumber <= cols.headerRow) return;

      const partyName = cellToString(getCellSafe(row, cols.partyCol));
      const soAgentName = cellToString(getCellSafe(row, cols.soAgentCol));
      const branchVal = cellToString(getCellSafe(row, cols.branchCol));
      const companyVal = cellToString(getCellSafe(row, cols.companyCol));
      const soOrderNo = cellToString(getCellSafe(row, cols.soOrderNoCol));
      const soOrderDate = cellToDateString(getCellSafe(row, cols.soOrderDateCol));
      const typeOfOrder = cellToString(getCellSafe(row, cols.typeCol));

      const isRowEmpty =
        partyName === ''
        && soAgentName === ''
        && branchVal === ''
        && companyVal === ''
        && soOrderNo === ''
        && (soOrderDate == null || soOrderDate === '')
        && typeOfOrder === '';

      if (isRowEmpty) return;
      // Do NOT fail upload for missing/blank fields on non-empty rows.

      valid.push({
        party_name: partyName.toUpperCase(),
        so_agent_name: soAgentName,
        branch: branchVal,
        company_name: companyVal,
        so_order_no: soOrderNo,
        so_order_date: soOrderDate,
        type_of_order: typeOfOrder,
        fy,
      });
    });

    if (valid.length === 0) {
      try {
        fs.unlinkSync(req.file.path);
      } catch {
        /* ignore */
      }
      return res.status(400).json({ success: false, error: 'No data rows found.' });
    }

    // Postgres: ON CONFLICT DO UPDATE cannot affect the same target row twice in one INSERT.
    // If the Excel contains duplicate PARTY NAME rows for the same brand+FY, dedupe them here.
    // Keep the last occurrence (latest Excel row wins).
    const dedupe = new Map();
    for (const r of valid) {
      const key = `${r.party_name}|${brand}|${fy}`;
      dedupe.set(key, r);
    }
    const dedupedValid = [...dedupe.values()];

    const client = await pool.connect();
    let inserted = 0;
    const bulkChunkSize = 500;
    try {
      await ensureSoMasterTable(client);
      await ensureUploadHistoryTable(client);
      await client.query('BEGIN');
      const baseSql = `
        INSERT INTO so_master (party_name, type_of_order, brand, fy)
        VALUES %VALUES%
        ON CONFLICT (party_name, brand, fy)
        DO UPDATE SET type_of_order = EXCLUDED.type_of_order
      `;

      for (let i = 0; i < dedupedValid.length; i += bulkChunkSize) {
        const chunk = dedupedValid.slice(i, i + bulkChunkSize);
        const params = [];
        const valuesSql = chunk.map((r, idx) => {
          const p = idx * 4;
          params.push(
            String(r.party_name).trim().toUpperCase(),
            r.type_of_order != null ? String(r.type_of_order).trim() : '',
            brand,
            fy,
          );
          return `($${p + 1}, $${p + 2}, $${p + 3}, $${p + 4})`;
        }).join(', ');

        const sql = baseSql.replace('%VALUES%', valuesSql);
        await client.query(sql, params);
        inserted += chunk.length;
      }
      await client.query('COMMIT');

      // Record success in history.
      await insertUploadHistory({
        client,
        filename: originalFilename,
        brand,
        fy,
        status: 'success',
        insertedRows: inserted,
        errorMessage: null,
      });
    } catch (e) {
      try {
        await client.query('ROLLBACK');
      } catch {
        /* ignore */
      }
      logError('admin', 'importSoMaster DB error', { message: e?.message });

      // Record failure in history (best-effort).
      try {
        await insertUploadHistory({
          client,
          filename: originalFilename,
          brand: canonicalizeBrand(brandRaw),
          fy,
          status: 'failed',
          insertedRows: inserted,
          errorMessage: e?.message || String(e),
        });
      } catch {
        /* ignore */
      }

      try {
        fs.unlinkSync(req.file.path);
      } catch {
        /* ignore */
      }
      return res.status(500).json({ success: false, error: e?.message || 'Database error.' });
    } finally {
      client.release();
    }

    try {
      fs.unlinkSync(req.file.path);
    } catch {
      /* ignore */
    }

    invalidateSoMasterCache();
    if (legacyTable) {
      try {
        await syncRowsToLegacyTable({ rows: dedupedValid, tableName: legacyTable, legacyBrand: brand });
      } catch (e) {
        logWarn('admin', `${legacyTable} sync exception`, { message: e?.message || String(e) });
      }
    }

    logInfo('admin', 'importSoMaster done', { inserted, brand, fy });
    return res.json({ success: true, inserted });
  } catch (err) {
    logError('admin', 'importSoMaster failed', { message: err?.message });
    if (req.file?.path) {
      try {
        fs.unlinkSync(req.file.path);
      } catch {
        /* ignore */
      }
    }
    return res.status(400).json({
      success: false,
      error: err?.message || 'Invalid file or parse error.',
    });
  }
}
