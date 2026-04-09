import fs from 'node:fs';
import path from 'node:path';
import { getPartyNameAliasKeys, normalizePartyName } from '../utils/normalizeHeader.js';

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

async function loadDistrictMasterMap(supabase) {
  const map = new Map();
  const pageSize = 1000;
  let from = 0;
  for (;;) {
    const to = from + pageSize - 1;
    const { data, error } = await supabase
      .from('district_master_data')
      .select('account_name, district')
      .range(from, to);
    if (error) throw new Error(error.message);
    for (const row of data || []) {
      const accountName = row?.account_name != null ? String(row.account_name).trim() : '';
      const districtVal = row?.district != null ? String(row.district).trim() : '';
      if (!accountName || !districtVal) continue;
      const key = normalizePartyName(accountName);
      if (!map.has(key)) map.set(key, districtVal);
      for (const alt of getPartyNameAliasKeys(accountName)) {
        if (alt && !map.has(alt)) map.set(alt, districtVal);
      }
    }
    if (!data || data.length < pageSize) break;
    from += pageSize;
  }
  return map;
}

function deriveDistrictFromToPartyName(toPartyName, districtMap) {
  if (!toPartyName) return null;
  const key = normalizePartyName(toPartyName);
  let district = districtMap.get(key);
  if (!district) {
    for (const alt of getPartyNameAliasKeys(String(toPartyName))) {
      district = districtMap.get(alt);
      if (district) break;
    }
  }
  return district ?? null;
}

async function main() {
  loadEnvFromFile(path.resolve(process.cwd(), 'backend', '.env'));
  const { supabase } = await import('../models/supabase.js');

  const districtMap = await loadDistrictMasterMap(supabase);
  console.log('district_master_data map size:', districtMap.size);

  const BATCH = 1000;
  let updated = 0;
  let scanned = 0;
  let lastId = 0;
  let firstNoMatchLogged = false;

  for (;;) {
    const { data: rows, error } = await supabase
      .from('sales_data')
      .select('id,to_party_name,district')
      .is('district', null)
      .gt('id', lastId)
      .order('id', { ascending: true })
      .limit(BATCH);
    if (error) throw new Error(error.message);
    if (!rows || rows.length === 0) break;

    scanned += rows.length;
    lastId = rows[rows.length - 1]?.id ?? lastId;

    const updates = [];
    for (const row of rows) {
      const district = deriveDistrictFromToPartyName(row.to_party_name, districtMap);
      if (district) {
        updates.push({ id: row.id, district });
      } else if (!firstNoMatchLogged) {
        firstNoMatchLogged = true;
        console.log('first no-match example:', {
          id: row.id,
          to_party_name: row.to_party_name,
          normalized: row.to_party_name ? normalizePartyName(row.to_party_name) : null,
        });
      }
    }

    if (updates.length > 0) {
      const u = await supabase.from('sales_data').upsert(updates, { onConflict: 'id' });
      if (u.error) throw new Error(u.error.message);
      updated += updates.length;
    }

    console.log(`scanned=${scanned} updated=${updated} batchUpdated=${updates.length} lastId=${lastId}`);
  }

  console.log('DONE. scanned=', scanned, 'updated=', updated, 'lastId=', lastId);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

