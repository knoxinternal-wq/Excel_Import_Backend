import { SALES_DATA_NUMERIC_COLUMNS } from '../config/constants.js';

/** Set of measure columns on `sales_data` (snake_case). */
export const SALES_DATA_NUMERIC_COLUMNS_SET = new Set(SALES_DATA_NUMERIC_COLUMNS);

/**
 * Parse a fact value for SUM/AVG/MIN/MAX and numeric filters (streaming pivot, JS).
 * Strips grouping commas (Indian / Excel style). `units_pack` allows only plain numeric text.
 * @param {unknown} value
 * @param {string} [field] sales_data column; affects units_pack handling
 * @returns {number|null}
 */
export function parseFactNumeric(value, field) {
  if (value == null || value === '') return null;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  const s = String(value).trim();
  if (!s) return null;
  if (field === 'units_pack') {
    const cleaned = s.replace(/,/g, '').trim();
    if (!/^[-+]?\d+(\.\d*)?$/.test(cleaned)) return null;
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : null;
  }
  const noComma = s.replace(/,/g, '');
  if (!noComma) return null;
  const n = Number(noComma);
  return Number.isNaN(n) ? null : n;
}

/**
 * Excel money/qty cells: strip commas and non-numeric junk (₹, spaces, etc.). Used at import.
 * @param {unknown} val
 * @returns {number|null}
 */
export function parseExcelMoneyQtyCell(val) {
  if (val === null || val === undefined || val === '') return null;
  if (typeof val === 'number' && Number.isFinite(val)) return val;
  const n = Number(String(val).replace(/,/g, '').replace(/[^\d.-]/g, ''));
  return Number.isFinite(n) ? n : null;
}
