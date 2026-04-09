/**
 * In-memory SO master map for import (UPPER(party)|UPPER(brand)|UPPER(fy) → type_of_order).
 * Source of truth: legacy per-brand master tables (dnj_so_master / ic_so_master / rf_so_master / vercelli_so_master).
 * This keeps imports fast (no runtime joins).
 */
import { getPgPool } from '../config/database.js';
import { getOrLoadMaster, invalidateMasterCache } from './masterLookupCache.js';
import { logWarn } from '../utils/logger.js';

const KEY_SO_MASTER = 'master:so_master_map_v1';

const SO_MASTER_TABLES = [
  { table: 'dnj_so_master', brand: 'DON AND JULIO' },
  { table: 'ic_so_master', brand: 'ITALIAN CHANNEL' },
  { table: 'rf_so_master', brand: 'RISHAB FABRICS' },
  { table: 'vercelli_so_master', brand: 'VERCELLI' },
];

/**
 * @returns {Promise<Map<string, string>>}
 */
export async function loadSoMasterMap() {
  const map = new Map();
  const pool = getPgPool();
  if (!pool) {
    logWarn('soMasterLoader', 'DATABASE_URL unset; SO master map empty');
    return map;
  }

  try {
    for (const { table } of SO_MASTER_TABLES) {
      // Each table has the canonical columns:
      //   party_name, type_of_order, brand, fy
      // Admin upload sets brand explicitly so the key matches sales import normalization.
      // If a table is missing (older deployments), we skip it gracefully.
      let rows = [];
      try {
        const res = await pool.query(`
          SELECT party_name, type_of_order, brand, fy
          FROM ${table}
        `);
        rows = res.rows || [];
      } catch (e) {
        logWarn('soMasterLoader', 'so master table skipped', { table, message: e?.message || String(e) });
        continue;
      }

      for (const r of rows) {
        const pn = r.party_name != null ? String(r.party_name).trim().toUpperCase() : '';
        const b = r.brand != null ? String(r.brand).trim().toUpperCase() : '';
        const fy = r.fy != null ? String(r.fy).trim().toUpperCase() : '';
        const t = r.type_of_order != null ? String(r.type_of_order).trim() : '';
        const key = `${pn}|${b}|${fy}`;
        if (pn && b && fy && t) map.set(key, t);
      }
    }
  } catch (e) {
    logWarn('soMasterLoader', 'load failed', { message: e?.message || String(e) });
  }

  return map;
}

export function getSoMasterMap() {
  return getOrLoadMaster(KEY_SO_MASTER, loadSoMasterMap);
}

export function invalidateSoMasterCache() {
  invalidateMasterCache(KEY_SO_MASTER);
}
