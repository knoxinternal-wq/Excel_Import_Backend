const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

const NUMERIC_KEYS = ['rate_unit', 'sl_qty', 'units_pack', 'gross_amount', 'amount_before_tax', 'net_amount'];
const DATE_KEYS = ['bill_date', 'sale_order_date'];

export function formatDate(val) {
  if (!val) return '-';
  const str = String(val).trim();
  let d;
  const ymd = str.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (ymd) {
    d = new Date(parseInt(ymd[1], 10), parseInt(ymd[2], 10) - 1, parseInt(ymd[3], 10));
  } else {
    d = new Date(val);
  }
  if (isNaN(d.getTime())) return val;
  const day = String(d.getDate()).padStart(2, '0');
  const mmm = MONTH_NAMES[d.getMonth()] || '';
  const year = d.getFullYear();
  return `${day}-${mmm}-${year}`;
}

export function formatNumber(val) {
  if (val == null || val === '') return '-';
  const n = typeof val === 'number' && Number.isFinite(val)
    ? val
    : Number(String(val).replace(/,/g, '').trim());
  return Number.isNaN(n) ? val : n.toLocaleString();
}

export function formatText(val) {
  if (val == null || val === '') return '-';
  const s = String(val).trim();
  return s || '-';
}

/** Derive FY (2025-26), MONTH (Apr-25), MMM (APR) from bill_date. */
export function deriveFYMonthFromBillDate(billDate) {
  if (billDate == null || billDate === '') return null;
  let year; let monthNum;
  if (billDate instanceof Date && !isNaN(billDate.getTime())) {
    year = billDate.getFullYear();
    monthNum = billDate.getMonth() + 1;
  } else {
    const s = String(billDate).trim();
    const ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})T/);
    const dmy = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
    const match = ymd || iso;
    if (match) {
      year = parseInt(match[1], 10);
      monthNum = parseInt(match[2], 10);
    } else if (dmy) {
      year = parseInt(dmy[3], 10);
      monthNum = parseInt(dmy[2], 10);
    } else {
      const n = Number(billDate);
      if (!isNaN(n) && n > 1000 && n < 100000) {
        const d = new Date((n - 25569) * 86400000);
        if (!isNaN(d.getTime())) {
          year = d.getFullYear();
          monthNum = d.getMonth() + 1;
        } else {
          return null;
        }
      } else {
        return null;
      }
    }
  }
  if (monthNum < 1 || monthNum > 12) return null;
  const fyYear = monthNum >= 4 ? year : year - 1;
  const mmmLabel = MONTH_NAMES[monthNum - 1] || '';
  return {
    fy: `${fyYear}-${String(fyYear + 1).slice(-2)}`,
    month: `${mmmLabel}-${String(year).slice(-2)}`,
    mmm: mmmLabel.toUpperCase(),
  };
}

/**
 * @param {Record<string, unknown>} row
 * @param {{ key: string }} col
 * @returns {string|number}
 */
export function getSalesCellDisplayValue(row, col) {
  const key = col.key;
  let val = row[key];

  if (key === 'district') return formatText(row.district);
  if (key === 'scheme') return '-';
  if (key === 'agent_name') return formatText(row.agent_name);
  if (key === 'agent_names_correction') return formatText(row.agent_names_correction);
  if (key === 'item_sub_cat') {
    return formatText(row.item_sub_cat ?? row.scheme);
  }
  if (key === 'goods_type') {
    return formatText(val).toUpperCase();
  }
  if (DATE_KEYS.includes(key)) return formatDate(val);
  if (NUMERIC_KEYS.includes(key)) return formatNumber(val);
  if (key === 'business_type') return formatText(val ?? row.businessType);

  if (['fy', 'month', 'mmm'].includes(key)) {
    const dateSrc = row.bill_date ?? row.billDate ?? row.sale_order_date ?? row.saleOrderDate;
    let derived = deriveFYMonthFromBillDate(dateSrc);
    if (!derived) {
      const numMonth = Number(row.month ?? row.MONTH);
      const fyStr = String(row.fy ?? row.FY ?? '').trim();
      const fyMatch = fyStr.match(/^(\d{4})-(\d{2})$/);
      if (fyMatch && numMonth >= 1 && numMonth <= 12) {
        const startYear = parseInt(fyMatch[1], 10);
        const y = numMonth >= 4 ? startYear : startYear + 1;
        const mmmLabel = MONTH_NAMES[numMonth - 1] || '';
        derived = {
          fy: fyStr,
          month: `${mmmLabel}-${String(y).slice(-2)}`,
          mmm: mmmLabel.toUpperCase(),
        };
      }
    }
    return derived?.[key] ?? formatText(val);
  }

  return formatText(val);
}

export { NUMERIC_KEYS };
