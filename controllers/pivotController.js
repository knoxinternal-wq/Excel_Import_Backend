import ExcelJS from 'exceljs';
import {
  getPivotFields,
  getPivotCapabilities,
  runPivot,
  runDrilldown,
  toDisplayNumber,
  getFilterValues,
  getFilterValuesBatch,
} from '../services/pivotService.js';
import { applyPivotBodyWindow } from '../services/pivotBodyOrder.js';
import { isPivotSqlStatementTimeoutError } from '../services/pivotSql.js';

const MAX_PIVOT_BODY_LIMIT = 100_000;

const PIVOT_SQL_TIMEOUT_MESSAGE =
  'Pivot SQL timed out on the database server. Add filters or use fewer row/column fields.';

function sendPivotSqlErrorResponse(res, err) {
  if (isPivotSqlStatementTimeoutError(err)) {
    return res.status(504).json({ error: PIVOT_SQL_TIMEOUT_MESSAGE, code: 'PIVOT_TIMEOUT' });
  }
  return res.status(400).json({ error: err.message });
}

function parsePivotBodyWindow(body) {
  const subtotalFields = Array.isArray(body?.subtotalFields) ? body.subtotalFields : [];
  const bodyOffset = Math.max(0, Number(body?.bodyOffset) || 0);
  const raw = body?.bodyLimit;
  if (raw == null || raw === '') return { subtotalFields, bodyOffset, bodyLimit: null };
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return { subtotalFields, bodyOffset, bodyLimit: null };
  return {
    subtotalFields,
    bodyOffset,
    bodyLimit: Math.min(MAX_PIVOT_BODY_LIMIT, Math.floor(n)),
  };
}

function stripPivotBodyWindowParams(config) {
  if (!config || typeof config !== 'object') return config;
  const next = { ...config };
  delete next.bodyOffset;
  delete next.bodyLimit;
  return next;
}

function csvEscape(v) {
  if (v == null) return '';
  const s = String(v);
  if (s.includes(',') || s.includes('"') || s.includes('\n')) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

/** Lakhs/crores-style grouping for CSV export (matches UI en-IN). */
function formatIndianNumericExportRows(rows, rowHeaderCount) {
  return rows.map((row) => row.map((cell, colIdx) => {
    if (colIdx < rowHeaderCount) return cell;
    if (typeof cell === 'number' && Number.isFinite(cell)) {
      return cell.toLocaleString('en-IN', { maximumFractionDigits: 0 });
    }
    return cell;
  }));
}

function metricLabel(metricKey) {
  const [agg, field] = String(metricKey).split(':');
  return `${String(agg || '').toUpperCase()} ${field || ''}`.trim();
}

function buildExportGrid(result) {
  const valueKeys = result.values.map((v) => `${v.agg}:${v.field}`);
  const rowHeaderLabels = result.config.rows.length ? result.config.rows : ['ROWS'];
  const columnHeaders = Array.isArray(result.columnHeaders) && result.columnHeaders.length
    ? result.columnHeaders
    : [{ key: '(all)', labels: ['(all)'] }];
  const colDepth = Math.max(1, ...columnHeaders.map((h) => (h.labels || []).length));

  const descriptors = [];
  for (const colHeader of columnHeaders) {
    const labels = Array.from({ length: colDepth }, (_, i) => colHeader.labels?.[i] ?? '');
    for (const metricKey of valueKeys) {
      descriptors.push({ kind: 'col', colKey: colHeader.key, metricKey, labels });
    }
  }
  for (const metricKey of valueKeys) {
    descriptors.push({
      kind: 'total',
      colKey: '__grand_total__',
      metricKey,
      labels: [...Array(Math.max(0, colDepth - 1)).fill(''), 'GRAND TOTAL'],
    });
  }

  const headerRows = [];
  for (let level = 0; level < colDepth; level += 1) {
    const lead = rowHeaderLabels.map((h, idx) => (level === colDepth - 1 ? String(h).toUpperCase() : (idx === 0 ? 'ROWS' : '')));
    const cols = descriptors.map((d) => String(d.labels[level] || '').toUpperCase());
    headerRows.push([...lead, ...cols]);
  }
  headerRows.push([
    ...rowHeaderLabels.map(() => ''),
    ...descriptors.map((d) => metricLabel(d.metricKey)),
  ]);

  const bodyRows = result.rowHeaders.map((rowHeader) => {
    const labels = rowHeader.labels?.length ? rowHeader.labels : ['(all)'];
    const lead = rowHeaderLabels.map((_, i) => labels[i] ?? (i === 0 ? '(all)' : ''));
    const vals = descriptors.map((d) => {
      const [agg] = d.metricKey.split(':');
      if (d.kind === 'total') {
        const bucket = result.rowTotals[rowHeader.key]?.[d.metricKey];
        return toDisplayNumber(bucket, agg, d.metricKey);
      }
      const bucket = result.cells[rowHeader.key]?.[d.colKey]?.[d.metricKey];
      return toDisplayNumber(bucket, agg, d.metricKey);
    });
    return [...lead, ...vals];
  });

  const grandTotalLead = rowHeaderLabels.map((_, i) => (i === 0 ? 'GRAND TOTAL' : ''));
  const grandTotalVals = descriptors.map((d) => {
    const [agg] = d.metricKey.split(':');
    if (d.kind === 'total') {
      const bucket = result.grandTotals[d.metricKey];
      return toDisplayNumber(bucket, agg, d.metricKey);
    }
    const bucket = result.columnTotals[d.colKey]?.[d.metricKey];
    return toDisplayNumber(bucket, agg, d.metricKey);
  });
  const footerRow = [...grandTotalLead, ...grandTotalVals];

  return {
    headerRows,
    bodyRows,
    footerRow,
    rowHeaderCount: rowHeaderLabels.length,
    descriptors,
  };
}

export async function getPivotFieldsHandler(req, res) {
  try {
    res.set('Cache-Control', 'public, max-age=300');
    res.json({ fields: getPivotFields() });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

export async function getPivotCapabilitiesHandler(req, res) {
  try {
    res.set('Cache-Control', 'public, max-age=120');
    res.json(getPivotCapabilities());
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

export async function getPivotDataHandler(req, res) {
  try {
    const body = req.body || {};
    const win = parsePivotBodyWindow(body);
    let result = await runPivot(body);
    if (win.bodyLimit != null) {
      result = applyPivotBodyWindow(result, {
        rows: result.config?.rows || [],
        subtotalFields: win.subtotalFields,
        bodyOffset: win.bodyOffset,
        bodyLimit: win.bodyLimit,
      }).result;
    }
    const valueKeys = result.values.map((v) => `${v.agg}:${v.field}`);
    const serialize = (metricsObj, metricKey) => {
      const [agg] = metricKey.split(':');
      return toDisplayNumber(metricsObj, agg, metricKey);
    };
    res.json({
      config: result.config,
      values: result.values,
      rowHeaders: result.rowHeaders,
      columnHeaders: result.columnHeaders,
      cells: Object.fromEntries(
        Object.entries(result.cells).map(([rk, cols]) => [
          rk,
          Object.fromEntries(
            Object.entries(cols).map(([ck, metrics]) => [
              ck,
              Object.fromEntries(valueKeys.map((mk) => [mk, serialize(metrics[mk], mk)])),
            ]),
          ),
        ]),
      ),
      rowTotals: Object.fromEntries(
        Object.entries(result.rowTotals).map(([rk, metrics]) => [
          rk,
          Object.fromEntries(valueKeys.map((mk) => [mk, serialize(metrics[mk], mk)])),
        ]),
      ),
      columnTotals: Object.fromEntries(
        Object.entries(result.columnTotals).map(([ck, metrics]) => [
          ck,
          Object.fromEntries(valueKeys.map((mk) => [mk, serialize(metrics[mk], mk)])),
        ]),
      ),
      grandTotals: Object.fromEntries(valueKeys.map((mk) => [mk, serialize(result.grandTotals[mk], mk)])),
      rowSubtotals: result.rowSubtotals.map((s) => ({
        depth: s.depth,
        key: s.key,
        labels: s.labels,
        cells: Object.fromEntries(
          Object.entries(s.cells).map(([ck, metrics]) => [
            ck,
            Object.fromEntries(valueKeys.map((mk) => [mk, serialize(metrics[mk], mk)])),
          ]),
        ),
        rowTotals: Object.fromEntries(
          valueKeys.map((mk) => [mk, serialize(s.rowTotals?.[mk], mk)]),
        ),
      })),
      meta: result.meta,
      ...(Array.isArray(result.bodyLines) && result.bodyLines.length
        ? { bodyLines: result.bodyLines }
        : {}),
    });
  } catch (err) {
    sendPivotSqlErrorResponse(res, err);
  }
}

export async function getPivotDrilldownHandler(req, res) {
  try {
    const { config, drill } = req.body || {};
    const result = await runDrilldown(config || {}, drill || {});
    res.json(result);
  } catch (err) {
    sendPivotSqlErrorResponse(res, err);
  }
}

export async function exportPivotHandler(req, res) {
  try {
    const body = req.body || {};
    const format = String(body.format || 'csv').toLowerCase();
    const result = await runPivot(stripPivotBodyWindowParams(body.config || {}));
    const grid = buildExportGrid(result);

    if (format === 'xlsx') {
      const wb = new ExcelJS.Workbook();
      const ws = wb.addWorksheet('Pivot');
      for (const row of grid.headerRows) ws.addRow(row);
      for (const row of grid.bodyRows) ws.addRow(row);
      ws.addRow(grid.footerRow);

      // Header styling
      for (let i = 1; i <= grid.headerRows.length; i += 1) {
        const row = ws.getRow(i);
        row.font = { bold: true };
        row.alignment = { vertical: 'middle', horizontal: 'center', wrapText: true };
        row.eachCell((cell) => {
          cell.fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: i === grid.headerRows.length ? 'FFDDE3EB' : 'FFE9EEF5' },
          };
          cell.border = {
            top: { style: 'thin', color: { argb: 'FFC7CED7' } },
            left: { style: 'thin', color: { argb: 'FFC7CED7' } },
            bottom: { style: 'thin', color: { argb: 'FFC7CED7' } },
            right: { style: 'thin', color: { argb: 'FFC7CED7' } },
          };
        });
      }

      // Body styling + numeric format
      const firstBodyRow = grid.headerRows.length + 1;
      const lastBodyRow = firstBodyRow + grid.bodyRows.length - 1;
      const grandTotalRow = lastBodyRow + 1;
      const rowHeaderCount = grid.rowHeaderCount;
      const totalCols = rowHeaderCount + grid.descriptors.length;

      for (let r = firstBodyRow; r <= grandTotalRow; r += 1) {
        const isTotal = r === grandTotalRow;
        const row = ws.getRow(r);
        row.eachCell((cell, colNumber) => {
          cell.border = {
            top: { style: 'thin', color: { argb: 'FFD3D9E1' } },
            left: { style: 'thin', color: { argb: 'FFD3D9E1' } },
            bottom: { style: 'thin', color: { argb: 'FFD3D9E1' } },
            right: { style: 'thin', color: { argb: 'FFD3D9E1' } },
          };
          cell.alignment = { vertical: 'middle', horizontal: colNumber <= rowHeaderCount ? 'left' : 'right' };
          if (isTotal) {
            cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFEFF3F8' } };
            if (colNumber <= rowHeaderCount || colNumber === totalCols) cell.font = { bold: true };
          }
        });
      }

      for (let c = rowHeaderCount + 1; c <= totalCols; c += 1) {
        ws.getColumn(c).numFmt = '#,##,##0.00';
      }

      // Column widths
      for (let c = 1; c <= totalCols; c += 1) {
        ws.getColumn(c).width = c <= rowHeaderCount ? 20 : 16;
      }

      ws.views = [{ state: 'frozen', ySplit: grid.headerRows.length, xSplit: rowHeaderCount }];

      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.setHeader('Content-Disposition', 'attachment; filename="pivot_report.xlsx"');
      const buf = await wb.xlsx.writeBuffer();
      return res.send(buf);
    }

    const bodyCsv = formatIndianNumericExportRows(grid.bodyRows, grid.rowHeaderCount);
    const [footerCsv] = formatIndianNumericExportRows([grid.footerRow], grid.rowHeaderCount);
    const csvRows = [...grid.headerRows, ...bodyCsv, footerCsv];
    const csv = csvRows.map((r) => r.map(csvEscape).join(',')).join('\n');
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="pivot_report.csv"');
    res.send(`\uFEFF${csv}`);
  } catch (err) {
    sendPivotSqlErrorResponse(res, err);
  }
}

export async function getPivotFilterValuesHandler(req, res) {
  try {
    const field = req.query.field;
    const search = req.query.search || '';
    const limit = req.query.limit || '';
    const values = await getFilterValues(field, search, limit);
    res.json({ field, values });
  } catch (err) {
    sendPivotSqlErrorResponse(res, err);
  }
}

/** POST body: `{ fields: string[], limit?: number }` — one round-trip for many pivot filter dropdowns. */
export async function getPivotFilterValuesBatchHandler(req, res) {
  try {
    const raw = req.body?.fields;
    const fields = Array.isArray(raw) ? raw : [];
    const limit = req.body?.limit;
    const batch = await getFilterValuesBatch(fields, limit);
    res.json({ fields: batch });
  } catch (err) {
    sendPivotSqlErrorResponse(res, err);
  }
}

export async function getPivotQuickHandler(req, res) {
  try {
    const rows = String(req.query.rows || 'region')
      .split(',')
      .map((x) => x.trim())
      .filter(Boolean);
    const columns = String(req.query.columns || '')
      .split(',')
      .map((x) => x.trim())
      .filter(Boolean);
    const field = String(req.query.field || 'net_amount').trim();
    const agg = String(req.query.agg || 'sum').trim().toLowerCase();
    const result = await runPivot({
      rows,
      columns,
      values: [{ field, agg, label: `${agg.toUpperCase()} ${field}` }],
      filters: {},
      limitRows: Number(req.query.limitRows) > 0 ? Number(req.query.limitRows) : undefined,
      sort: { rows: 'asc', columns: 'asc' },
    });
    res.set('Cache-Control', 'private, max-age=30');
    res.json(result);
  } catch (err) {
    sendPivotSqlErrorResponse(res, err);
  }
}

