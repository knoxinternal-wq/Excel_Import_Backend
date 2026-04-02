import { supabase } from '../models/supabase.js';
import ExcelJS from 'exceljs';
import { normalizePartyName, getPartyNameAliasKeys } from '../utils/normalizeHeader.js';
import { getPartyGroupingMasterMap } from '../services/masterLoaders.js';
import { getReportMetaPayload } from '../services/reportMetaService.js';

const CSV_COLUMNS = [
  'branch',
  'fy',
  'month',
  'mmm',
  'region',
  'state',
  'district',
  'city',
  'business_type',
  'agent_names_correction',
  'party_grouped',
  'party_name_for_count',
  'brand',
  'agent_name',
  'to_party_name',
  'bill_no',
  'bill_date',
  'item_no',
  'shade_name',
  'rate_unit',
  'size',
  'units_pack',
  'sl_qty',
  'gross_amount',
  'amount_before_tax',
  'net_amount',
  'sale_order_no',
  'sale_order_date',
  'item_with_shade',
  'item_category',
  'item_sub_cat',
  'so_type',
  'scheme',
  'goods_type',
  'agent_name_final',
  'pin_code',
];

function enrichPartyGroupingForReport(row, partyGroupingMap) {
  const toPartyName = row?.to_party_name != null ? String(row.to_party_name).trim() : '';
  if (!toPartyName) return row;
  const key = normalizePartyName(toPartyName);
  let master = partyGroupingMap?.get(key);
  if (!master) {
    for (const alt of getPartyNameAliasKeys(toPartyName)) {
      master = partyGroupingMap?.get(alt);
      if (master) break;
    }
  }
  if (!master) return row;
  return {
    ...row,
    party_grouped: master.party_grouped ?? toPartyName,
    party_name_for_count: master.party_name_for_count ?? row.party_name_for_count ?? toPartyName,
  };
}

export async function getReportMeta(req, res) {
  try {
    const payload = await getReportMetaPayload();
    res.json(payload);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

export async function downloadSalesReport(req, res) {
  try {
    const mode = String(req.query.mode || '').trim(); // 'monthly' | 'yearly'
    const fy = String(req.query.fy || '').trim();
    const month = String(req.query.month || '').trim();

    if (!fy) return res.status(400).json({ error: 'Missing fy' });
    if (mode !== 'monthly' && mode !== 'yearly') return res.status(400).json({ error: 'Invalid mode' });
    if (mode === 'monthly' && !month) return res.status(400).json({ error: 'Missing month' });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    const safeFy = fy.replace(/[^a-zA-Z0-9_-]/g, '_');
    const safeMonth = month ? month.replace(/[^a-zA-Z0-9_-]/g, '_') : '';
    const fileName = `sales_report_${mode}_${safeFy}${safeMonth ? `_${safeMonth}` : ''}.xlsx`;
    res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);

    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Sales Data');
    const partyGroupingMap = await getPartyGroupingMasterMap();

    const toExcelHeader = (key) =>
      String(key)
        .replace(/_/g, ' ')
        .toUpperCase();

    // Avoid ExcelJS auto-header generation from `worksheet.columns` keys.
    // We add the header row manually so we can control uppercase + bold exactly.
    const headerRow = worksheet.addRow(CSV_COLUMNS.map((c) => toExcelHeader(c)));
    headerRow.font = { bold: true };
    CSV_COLUMNS.forEach((_, idx) => {
      worksheet.getColumn(idx + 1).width = 20;
    });

    // Keyset pagination ensures we export *all* matching rows.
    // Offset-based pagination can be truncated by PostgREST limits, causing early termination.
    const PAGE_SIZE = 1000;
    let lastId = null;

    for (;;) {
      let q = supabase
        .from('sales_data')
        .select(['id', ...CSV_COLUMNS].join(','))
        .eq('fy', fy);
      if (mode === 'monthly') q = q.eq('month', month);
      if (lastId != null) q = q.gt('id', lastId);

      q = q.order('id', { ascending: true }).limit(PAGE_SIZE);

      const { data, error } = await q;
      if (error) throw new Error(error.message);

      const rows = data || [];
      if (rows.length === 0) break;

      for (const r of rows) {
        const enriched = enrichPartyGroupingForReport(r, partyGroupingMap);
        worksheet.addRow(CSV_COLUMNS.map((c) => enriched[c]));
      }

      lastId = rows[rows.length - 1]?.id ?? null;
    }

    const buffer = await workbook.xlsx.writeBuffer();
    res.send(buffer);
  } catch (err) {
    try {
      if (!res.headersSent) res.status(500).json({ error: err.message });
      else res.end();
    } catch {
      res.end();
    }
  }
}

