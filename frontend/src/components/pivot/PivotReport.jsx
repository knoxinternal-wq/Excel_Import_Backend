import {
  useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState,
  memo,
} from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import {
  Loader2,
  Rows3,
  Columns3,
  Sigma,
  Funnel,
  FileSpreadsheet,
  X,
  Search,
  PanelRightClose,
  PanelRightOpen,
  GripVertical,
} from 'lucide-react';
import { pivotApi, peekPivotFilterValuesCache } from '../../services/api';
import { MAX_SALES_ROWS } from '../../constants/limits';
import {
  LONG_RUNNING_REQUEST_MS,
  MAX_UI_DATA_LOAD_MS,
  PIVOT_FILTER_VALUES_TIMEOUT_MS,
} from '../../constants/timing';
import { formatRequestError } from '../../utils/requestError';

const AGG_OPTIONS = ['sum', 'count', 'avg', 'min', 'max'];
const PIVOT_CONFIG_KEY = 'pivot_report_config_v1';
/** Excel-style report filters: multi-select only (`in`). */
function normalizeFilterToMultiOnly(f) {
  if (!f?.field) return null;
  const op = String(f.operator || 'eq').toLowerCase();
  if (op === 'in') {
    return {
      field: f.field,
      operator: 'in',
      values: Array.isArray(f.values) ? f.values : [],
      value: '',
    };
  }
  if (op === 'is_blank' || op === 'is_not_blank') {
    return { field: f.field, operator: 'in', values: [], value: '' };
  }
  const single = String(f.value ?? '').trim();
  return {
    field: f.field,
    operator: 'in',
    values: single ? [single] : [],
    value: '',
  };
}

function summarizePivotFilter(f) {
  const n = Array.isArray(f?.values) ? f.values.length : 0;
  if (n === 0) return 'In (none)';
  return `In (${n} selected)`;
}

/** Append a windowed pivot body chunk; column/grand totals stay from the first page. */
function mergePivotBodyPages(prev, nextChunk, pageSize) {
  if (!prev) return nextChunk;
  if (!nextChunk?.bodyLines?.length) return prev;
  const mergedLines = [...(prev.bodyLines || []), ...nextChunk.bodyLines];
  const nextBp = nextChunk.meta?.bodyPaging;
  const totalLines = nextBp?.totalLines ?? prev.meta?.bodyPaging?.totalLines ?? mergedLines.length;
  return {
    ...nextChunk,
    rowHeaders: [...(prev.rowHeaders || []), ...(nextChunk.rowHeaders || [])],
    cells: { ...(prev.cells || {}), ...(nextChunk.cells || {}) },
    rowTotals: { ...(prev.rowTotals || {}), ...(nextChunk.rowTotals || {}) },
    rowSubtotals: [...(prev.rowSubtotals || []), ...(nextChunk.rowSubtotals || [])],
    bodyLines: mergedLines,
    grandTotals: prev.grandTotals,
    columnTotals: prev.columnTotals,
    columnHeaders: prev.columnHeaders,
    config: prev.config,
    values: prev.values,
    meta: {
      ...prev.meta,
      ...nextChunk.meta,
      sourceRows: prev.meta?.sourceRows ?? nextChunk.meta?.sourceRows,
      filteredRows: prev.meta?.filteredRows ?? nextChunk.meta?.filteredRows,
      visibleCells: prev.meta?.visibleCells ?? nextChunk.meta?.visibleCells,
      engine: prev.meta?.engine ?? nextChunk.meta?.engine,
      warnings: prev.meta?.warnings ?? nextChunk.meta?.warnings,
      executionMs: prev.meta?.executionMs ?? nextChunk.meta?.executionMs,
      memFiltersCount: prev.meta?.memFiltersCount ?? nextChunk.meta?.memFiltersCount,
      bodyPaging: {
        totalLines,
        offset: 0,
        limit: pageSize,
        truncatedAfter: Boolean(nextBp?.truncatedAfter),
        loadedLines: mergedLines.length,
      },
    },
  };
}

function filtersArrayFromStored(raw) {
  let rows = [];
  if (Array.isArray(raw)) rows = raw;
  else if (raw && typeof raw === 'object') {
    rows = Object.entries(raw).map(([field, spec]) => ({
      field,
      operator: spec?.operator || 'eq',
      value: spec?.value ?? '',
      values: Array.isArray(spec?.values) ? spec.values : undefined,
    }));
  }
  return rows.map(normalizeFilterToMultiOnly).filter(Boolean);
}

/** Stable API payload: array preserves multiple filters and duplicate fields (AND). */
function filtersToPayloadArray(filters) {
  return (filters || [])
    .filter((f) => f?.field)
    .map((f) => ({
      field: f.field,
      operator: 'in',
      value: '',
      values: Array.isArray(f.values) ? f.values : [],
    }));
}

function getInvalidFilterMessage(filters) {
  for (const f of filters || []) {
    if (!f?.field) continue;
    const label = fieldLabel(f.field);
    if (!Array.isArray(f.values) || f.values.length === 0) {
      return `Select one value in filter: ${label}`;
    }
  }
  return '';
}

// Match labels used in Sales Data table header (VirtualizedTable columns).
const SALES_HEADER_LABELS = {
  branch: 'BRANCH',
  fy: 'FY',
  month: 'MONTH',
  mmm: 'MMM',
  region: 'REGION',
  state: 'STATE',
  district: 'DISTRICT',
  city: 'CITY',
  business_type: 'TYPE OF BUSINESS',
  agent_names_correction: 'AGENT NAMES CORRECTION',
  party_grouped: 'PARTY GROUPED',
  party_name_for_count: 'PARTY NAME FOR COUNT',
  brand: 'BRAND',
  agent_name: 'AGENT NAME',
  to_party_name: 'TO PARTY NAME',
  bill_no: 'BILL NO.',
  bill_date: 'BILL DATE',
  item_no: 'ITEM NO',
  shade_name: 'SHADE NAME',
  rate_unit: 'RATE/UNIT',
  size: 'SIZE',
  units_pack: 'UNITS/PACK',
  sl_qty: 'SL QTY',
  gross_amount: 'GROSS AMOUNT',
  amount_before_tax: 'AMOUNT BEFORE TAX',
  net_amount: 'NET AMOUNT',
  sale_order_no: 'SALE ORDER NO.',
  sale_order_date: 'SALE ORDER DATE',
  item_with_shade: 'ITEM WITH SHADE',
  item_category: 'ITEM CATEGORY',
  item_sub_cat: 'ITEM SUB CAT',
  so_type: 'SO TYPE',
  scheme: 'SCHEME',
  goods_type: 'GOODS TYPE',
  agent_name_final: 'AGENT NAME.',
  pin_code: 'PIN CODE',
};

function toUpperLabel(v) {
  return String(v ?? '').toUpperCase();
}

function parseErr(e, fallback, timeoutMs = MAX_UI_DATA_LOAD_MS) {
  return formatRequestError(e, fallback, { timeoutMs });
}

/** Indian digit grouping (lakhs/crores). Pivot amounts are whole numbers (half-up); no decimal tail in UI. */
function formatPivotIndian(val) {
  if (val == null || val === '') return '-';
  if (typeof val !== 'number' || !Number.isFinite(val)) return String(val);
  const n = Number(val.toPrecision(15));
  const rounded = Math.sign(n) * Math.round(Math.abs(n) + 1e-9);
  return rounded.toLocaleString('en-IN', { maximumFractionDigits: 0 });
}

function readStorageJSON(key, fallback) {
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch {
    return fallback;
  }
}

/** Default row/column label order sent to the API (smart date/month sort on server). */
const DEFAULT_PIVOT_SORT = Object.freeze({ rows: 'asc', columns: 'asc' });

function pillLabel(v) {
  return String(v ?? '').replaceAll('_', ' ').toUpperCase();
}

function fieldLabel(v) {
  const key = String(v ?? '').trim();
  return SALES_HEADER_LABELS[key] || pillLabel(key);
}

function metricHeaderLabel(metricKey) {
  const [agg, field] = String(metricKey || '').split(':');
  const aggLabel = String(agg || '').toUpperCase();
  return `${aggLabel} ${fieldLabel(field)}`.trim();
}

function displayPivotAxisLabel(value, fallback = '(all)') {
  const text = String(value ?? '').trim();
  if (!text) return fallback;
  return text;
}

function buildColumnHeaderRows(columnHeaders = []) {
  const safeHeaders = Array.isArray(columnHeaders) && columnHeaders.length
    ? columnHeaders
    : [{ key: '(all)', labels: ['(all)'] }];
  const depth = Math.max(1, ...safeHeaders.map((h) => (Array.isArray(h.labels) ? h.labels.length : 0)));
  const rows = [];

  for (let level = 0; level < depth; level += 1) {
    const groups = [];
    let i = 0;
    while (i < safeHeaders.length) {
      const current = safeHeaders[i];
      const parentPrefix = (current.labels || []).slice(0, level).join('||');
      const label = (current.labels || [])[level] ?? '(all)';
      let span = 1;
      i += 1;
      while (i < safeHeaders.length) {
        const next = safeHeaders[i];
        const nextParentPrefix = (next.labels || []).slice(0, level).join('||');
        const nextLabel = (next.labels || [])[level] ?? '(all)';
        if (nextParentPrefix === parentPrefix && nextLabel === label) {
          span += 1;
          i += 1;
        } else {
          break;
        }
      }
      groups.push({ label, span });
    }
    rows.push(groups);
  }

  return { depth, rows };
}

/** Rows per API page; grand totals always come from full aggregation. Next pages reuse server pivot cache. */
const PIVOT_BODY_PAGE_SIZE = 100;
/** Default / minimum row height before dynamic measure (wrapped text grows rows). */
const PIVOT_BODY_ROW_HEIGHT = 36;
const FILTER_RUN_DEBOUNCE_MS = 1100;
/** Longer TTL repeats same layout/filter faster without hitting the API. */
const PIVOT_CLIENT_CACHE_TTL_MS = 120_000;
const FILTER_VALUES_LIMIT = 500;

function pivotColumnWidths(rowHeaderCount, descriptorCount) {
  const w = [];
  for (let i = 0; i < rowHeaderCount; i += 1) w.push(i === 0 ? 200 : 168);
  for (let i = 0; i < descriptorCount; i += 1) w.push(120);
  return w;
}

function widthFromHeaderText(text, { min = 82, max = 220, pad = 26 } = {}) {
  const len = String(text ?? '').trim().length;
  // Approx monospace-ish width at 9-10px table text.
  const estimate = Math.round(len * 7.1) + pad;
  return Math.max(min, Math.min(max, estimate));
}

/** Stretch column mins to fill `targetWidth` (px); extra space shared in proportion to min widths. */
function expandColumnWidthsToTarget(mins, targetWidth) {
  const minSum = mins.reduce((a, x) => a + x, 0);
  if (!minSum || targetWidth <= minSum) return [...mins];
  const extra = targetWidth - minSum;
  const out = mins.map((w) => Math.floor(w + (extra * w) / minSum));
  let remainder = Math.round(targetWidth) - out.reduce((a, b) => a + b, 0);
  let i = out.length - 1;
  while (remainder > 0 && i >= 0) {
    out[i] += 1;
    remainder -= 1;
    i -= 1;
  }
  return out;
}

/** Match backend readMetric: resolve API number or raw aggregate bucket to a display number. */
function pivotCellNumericValue(val, metricKey) {
  if (val == null || val === '') return null;
  if (typeof val === 'number' && Number.isFinite(val)) return val;
  if (typeof val === 'object' && val !== null && ('sum' in val || 'count' in val)) {
    const agg = String(metricKey || '').split(':')[0]?.toLowerCase() || 'sum';
    if (agg === 'sum') return typeof val.sum === 'number' && Number.isFinite(val.sum) ? val.sum : null;
    if (agg === 'count') return typeof val.count === 'number' && Number.isFinite(val.count) ? val.count : null;
    if (agg === 'avg') {
      const c = val.count;
      const s = val.sum;
      if (typeof c === 'number' && c > 0 && typeof s === 'number' && Number.isFinite(s)) return s / c;
      return null;
    }
    if (agg === 'min') return val.min != null && Number.isFinite(Number(val.min)) ? Number(val.min) : null;
    if (agg === 'max') return val.max != null && Number.isFinite(Number(val.max)) ? Number(val.max) : null;
  }
  return null;
}

/** Subtotal row “grand total” column: prefer API rowTotals; else merge per-column buckets (older cached results). */
function subtotalRowTotalNumeric(st, metricKey) {
  const fromTotals = pivotCellNumericValue(st?.rowTotals?.[metricKey], metricKey);
  if (fromTotals != null && Number.isFinite(fromTotals)) return fromTotals;
  if (!st?.cells) return null;
  const merged = { sum: 0, count: 0, min: null, max: null };
  let any = false;
  for (const metrics of Object.values(st.cells)) {
    const b = metrics?.[metricKey];
    if (b == null) continue;
    if (typeof b === 'number' && Number.isFinite(b)) {
      merged.sum += b;
      merged.count += 1;
      any = true;
    } else if (typeof b === 'object' && b !== null && ('sum' in b || 'count' in b)) {
      merged.sum += Number(b.sum) || 0;
      merged.count += Number(b.count) || 0;
      merged.min = merged.min == null ? b.min : (b.min == null ? merged.min : Math.min(merged.min, b.min));
      merged.max = merged.max == null ? b.max : (b.max == null ? merged.max : Math.max(merged.max, b.max));
      any = true;
    }
  }
  if (!any) return null;
  return pivotCellNumericValue(merged, metricKey);
}

function formatPivotCellValue(val, formatPivotIndian, metricKey) {
  const n = pivotCellNumericValue(val, metricKey);
  if (n != null && Number.isFinite(n)) return formatPivotIndian(n);
  if (val == null || val === '') return '-';
  if (typeof val === 'string') return val;
  return '-';
}

function useDebouncedValue(value, delayMs) {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setDebounced(value), delayMs);
    return () => clearTimeout(t);
  }, [value, delayMs]);
  return debounced;
}

const PivotVirtualRow = memo(function PivotVirtualRow({
  index,
  style,
  ariaAttributes = {},
  orderedBodyRows,
  rowHeaderFields,
  descriptors,
  rowTotals,
  cells,
  formatPivotIndian,
  isSelectedCell,
  onCellClick,
  gridTemplateColumns,
}) {
  const entry = orderedBodyRows[index];
  if (!entry) return null;
  const gridRow = index + 1;

  if (entry.type === 'row') {
    const rh = entry.row;
    return (
      <div
        {...ariaAttributes}
        style={{ ...style, display: 'grid', gridTemplateColumns, boxSizing: 'border-box' }}
        className={`items-stretch border-b border-slate-200 ${index % 2 === 0 ? 'bg-white' : 'bg-slate-50/40'} hover:bg-slate-50`}
      >
        {rowHeaderFields.map((_, idx) => {
          const val = String(rh.labels?.[idx] ?? (idx === 0 ? '(all)' : '')).toUpperCase();
          return (
            <div
              key={`${rh.key}-r-${idx}`}
              className={[
                'min-h-full min-w-0 border-r border-slate-200 px-1.5 py-1.5 text-left text-[10px] font-semibold leading-snug text-slate-700 [overflow-wrap:anywhere] break-words whitespace-normal',
                isSelectedCell(gridRow, idx) ? 'bg-blue-100' : '',
              ].join(' ')}
              onClick={() => onCellClick(gridRow, idx)}
              role="presentation"
            >
              {val || '-'}
            </div>
          );
        })}
        {descriptors.map((d, dIdx) => (
          <div
            key={`${rh.key}-${d.kind}-${d.colKey}-${d.metricKey}`}
            className={[
              'min-h-full min-w-0 border-r border-slate-200 bg-white px-1.5 py-1.5 text-right text-[10px] tabular-nums leading-snug [overflow-wrap:anywhere] break-words whitespace-normal',
              dIdx === descriptors.length - 1 ? 'pr-4 sm:pr-5' : '',
              isSelectedCell(gridRow, rowHeaderFields.length + dIdx) ? 'bg-blue-100' : '',
            ].join(' ')}
            onClick={() => onCellClick(gridRow, rowHeaderFields.length + dIdx)}
            role="presentation"
          >
            {d.kind === 'total'
              ? formatPivotCellValue(rowTotals?.[rh.key]?.[d.metricKey], formatPivotIndian, d.metricKey)
              : formatPivotCellValue(cells?.[rh.key]?.[d.colKey]?.[d.metricKey], formatPivotIndian, d.metricKey)}
          </div>
        ))}
      </div>
    );
  }

  const st = entry.subtotal;
  return (
    <div
      {...ariaAttributes}
      style={{ ...style, display: 'grid', gridTemplateColumns, boxSizing: 'border-box' }}
      className="items-stretch border-b border-amber-200/80 bg-amber-50/80"
    >
      {rowHeaderFields.map((_, idx) => {
        const val = String(st.labels?.[idx] ?? (idx === 0 ? 'SUBTOTAL' : '')).toUpperCase();
        return (
          <div
            key={`st-${st.key}-r-${idx}`}
            className={[
              'min-h-full min-w-0 border-r border-amber-200/60 px-1.5 py-1.5 text-left text-[10px] font-bold leading-snug text-amber-900 [overflow-wrap:anywhere] break-words whitespace-normal',
              isSelectedCell(gridRow, idx) ? 'bg-blue-100' : '',
            ].join(' ')}
            onClick={() => onCellClick(gridRow, idx)}
            role="presentation"
          >
            {val || '-'}
          </div>
        );
      })}
      {descriptors.map((d, dIdx) => (
        <div
          key={`st-${st.key}-${d.kind}-${d.colKey}-${d.metricKey}`}
          className={[
            'min-h-full min-w-0 border-r border-amber-200/60 bg-amber-50 px-1.5 py-1.5 text-right text-[10px] font-semibold tabular-nums leading-snug [overflow-wrap:anywhere] break-words whitespace-normal',
            dIdx === descriptors.length - 1 ? 'pr-4 sm:pr-5' : '',
            isSelectedCell(gridRow, rowHeaderFields.length + dIdx) ? 'bg-blue-100' : '',
          ].join(' ')}
          onClick={() => onCellClick(gridRow, rowHeaderFields.length + dIdx)}
          role="presentation"
        >
          {d.kind === 'total'
            ? formatPivotCellValue(
              subtotalRowTotalNumeric(st, d.metricKey),
              formatPivotIndian,
              d.metricKey,
            )
            : formatPivotCellValue(st.cells?.[d.colKey]?.[d.metricKey], formatPivotIndian, d.metricKey)}
        </div>
      ))}
    </div>
  );
});

function buildPivotLayout(result) {
  if (!result) {
    return { descriptors: [], labelRows: [], metricRow: [], headerDepth: 1 };
  }

  const valueKeys = (result.values || []).map((v) => `${v.agg}:${v.field}`);
  const columnHeaders = Array.isArray(result.columnHeaders) && result.columnHeaders.length
    ? result.columnHeaders
    : [{ key: '(all)', labels: ['(all)'] }];
  const hasRealColumnAxis = Array.isArray(result?.config?.columns) && result.config.columns.length > 0;
  const labelDepth = Math.max(1, ...columnHeaders.map((h) => (h.labels || []).length));

  const descriptors = [];
  for (const colHeader of columnHeaders) {
    const labels = Array.from(
      { length: labelDepth },
      (_, i) => displayPivotAxisLabel(colHeader.labels?.[i], '(all)'),
    );
    for (const metricKey of valueKeys) {
      descriptors.push({ kind: 'col', colKey: colHeader.key, metricKey, labels });
    }
  }
  // If there is no real column axis, "(all)" already represents the grand total bucket.
  // Avoid duplicate "(all)" + "GRAND TOTAL" measure columns in UI.
  if (hasRealColumnAxis) {
    for (const metricKey of valueKeys) {
      descriptors.push({
        kind: 'total',
        colKey: '__grand_total__',
        metricKey,
        // Avoid a visually blank header cell above the grand-total column on multi-level column axes.
        labels: Array.from({ length: labelDepth }, () => 'GRAND TOTAL'),
      });
    }
  }

  const labelRows = [];
  for (let level = 0; level < labelDepth; level += 1) {
    const groups = [];
    let i = 0;
    while (i < descriptors.length) {
      const cur = descriptors[i];
      const parentPrefix = cur.labels.slice(0, level).join('||');
      const label = cur.labels[level] ?? '';
      let span = 1;
      i += 1;
      while (i < descriptors.length) {
        const nxt = descriptors[i];
        const nxtParent = nxt.labels.slice(0, level).join('||');
        const nxtLabel = nxt.labels[level] ?? '';
        if (nxtParent === parentPrefix && nxtLabel === label) {
          span += 1;
          i += 1;
        } else {
          break;
        }
      }
      groups.push({ label, span });
    }
    labelRows.push(groups);
  }

  const metricRow = descriptors.map((d) => metricHeaderLabel(d.metricKey));
  return { descriptors, labelRows, metricRow, headerDepth: labelDepth + 1 };
}

export default function PivotReport() {
  const savedConfig = readStorageJSON(PIVOT_CONFIG_KEY, {});
  const [fields, setFields] = useState([]);
  const [loadingFields, setLoadingFields] = useState(false);
  const [error, setError] = useState(null);

  const [rows, setRows] = useState(() => (Array.isArray(savedConfig?.rows) ? savedConfig.rows : []));
  const [columns, setColumns] = useState(() => (Array.isArray(savedConfig?.columns) ? savedConfig.columns : []));
  const [values, setValues] = useState(() => (Array.isArray(savedConfig?.values) ? savedConfig.values : []));
  const [filters, setFilters] = useState(() => filtersArrayFromStored(savedConfig?.filters));
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState(null);
  const [subtotalFields, setSubtotalFields] = useState(() => (Array.isArray(savedConfig?.subtotalFields) ? savedConfig.subtotalFields : []));
  const [filterOptions, setFilterOptions] = useState({});
  const [filterLoading, setFilterLoading] = useState({});
  const [filterErrorByField, setFilterErrorByField] = useState({});
  /** True after a fetch attempt finished (success or error) for that field. */
  const [filterReady, setFilterReady] = useState({});
  /** One successful fetch per field (even if empty); avoids refetch loops. Use force to reload. */
  const filterFetchedRef = useRef(new Set());
  /** Ignores stale batch responses when filter fields change quickly. */
  const filterBatchGenRef = useRef(0);
  const [toolsCollapsed, setToolsCollapsed] = useState(false);
  const [fieldSearch, setFieldSearch] = useState('');
  const [rowSearch, setRowSearch] = useState('');
  const [dragItem, setDragItem] = useState(null);
  const [dropTarget, setDropTarget] = useState('');
  /** Invisible drag image reduces browser drag-overlay work and makes drops feel snappier. */
  const dragGhostRef = useRef(null);
  const [exportingPivot, setExportingPivot] = useState(false);
  const tableWrapRef = useRef(null);
  const pivotGridRef = useRef(null);
  /** Fills space between pivot header and grand-total row; height drives virtualized body. */
  const pivotListWrapRef = useRef(null);
  /** Single horizontal scroll for header + virtualized body + footer (avoids stacked scrollbars). */
  const pivotHScrollRef = useRef(null);
  const pivotSectionRef = useRef(null);
  const runSeqRef = useRef(0);
  const lastRunKeyRef = useRef('');
  const pivotRequestAbortRef = useRef(null);
  const loadMoreAbortRef = useRef(null);
  const pivotLoadMoreInflightRef = useRef(false);
  const pivotResponseCacheRef = useRef(new Map());
  const [bodyLoadingMore, setBodyLoadingMore] = useState(false);
  const [selectionStart, setSelectionStart] = useState(null);
  const [selectionEnd, setSelectionEnd] = useState(null);
  const [copyStatus, setCopyStatus] = useState('');
  const [pivotLoadAll, setPivotLoadAll] = useState(false);
  const debouncedFilters = useDebouncedValue(filters, FILTER_RUN_DEBOUNCE_MS);
  const debouncedFiltersPayload = useMemo(
    () => filtersToPayloadArray(debouncedFilters),
    [debouncedFilters],
  );
  /** Match backend: rows/columns without Values → Count of rows (Excel-style). */
  const valuesForPivot = useMemo(() => {
    if (values.length > 0) return values;
    if (rows.length > 0 || columns.length > 0) {
      return [{ field: 'id', agg: 'count', label: 'Count of rows' }];
    }
    return [];
  }, [values, rows, columns]);
  const [listWrapWidth, setListWrapWidth] = useState(960);
  /** Measured height of #pivotListWrapRef so body + grand total fit in the pivot viewport. */
  const [pivotListViewportPx, setPivotListViewportPx] = useState(320);

  const fieldByName = useMemo(() => Object.fromEntries(fields.map((f) => [f.field, f])), [fields]);
  const filterFields = useMemo(
    () => fields.filter((f) => !['id', 'created_at'].includes(String(f.field || '').toLowerCase())),
    [fields],
  );
  const filteredFieldPool = useMemo(() => {
    const q = fieldSearch.trim().toLowerCase();
    if (!q) return filterFields;
    return filterFields.filter((f) => {
      const name = String(f.field || '').toLowerCase();
      const label = fieldLabel(f.field).toLowerCase();
      return name.includes(q) || label.includes(q);
    });
  }, [filterFields, fieldSearch]);
  const filteredRowItems = useMemo(() => {
    const q = rowSearch.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter((r) => fieldLabel(r).toLowerCase().includes(q) || String(r).toLowerCase().includes(q));
  }, [rows, rowSearch]);
  const valueKeys = useMemo(() => (result?.values || []).map((v) => `${v.agg}:${v.field}`), [result]);
  const pivotLayout = useMemo(() => buildPivotLayout(result), [result]);
  const rowHeaderFields = useMemo(() => (rows.length ? rows : ['(all)']), [rows]);
  const subtotalDepthSet = useMemo(() => {
    const depths = subtotalFields
      .map((f) => rows.indexOf(f) + 1)
      .filter((d) => d > 0);
    return new Set(depths);
  }, [subtotalFields, rows]);
  const visibleSubtotals = useMemo(
    () => (result?.rowSubtotals || []).filter((st) => subtotalDepthSet.has(st.depth)),
    [result, subtotalDepthSet],
  );
  const orderedBodyRows = useMemo(() => {
    if (Array.isArray(result?.bodyLines) && result.bodyLines.length > 0) {
      return result.bodyLines.map((line) => {
        if (line.type === 'row') {
          const rh = line.row;
          return { type: 'row', key: `row-${rh.key}`, row: rh };
        }
        const st = line.subtotal;
        return { type: 'subtotal', key: `subtotal-${st.key}`, subtotal: st };
      });
    }
    const rowHeaders = result?.rowHeaders || [];
    if (!rowHeaders.length) return [];
    if (!visibleSubtotals.length || subtotalDepthSet.size === 0) {
      return rowHeaders.map((rh) => ({ type: 'row', key: `row-${rh.key}`, row: rh }));
    }

    const subtotalByKey = new Map(visibleSubtotals.map((st) => [st.key, st]));
    const selectedDepthsDesc = [...subtotalDepthSet].sort((a, b) => b - a);
    const out = [];

    const prefixKey = (labels, depth) => [...labels.slice(0, depth), '__subtotal__'].join('||');
    const prefix = (labels, depth) => labels.slice(0, depth).join('||');

    for (let i = 0; i < rowHeaders.length; i += 1) {
      const rh = rowHeaders[i];
      const next = rowHeaders[i + 1];
      out.push({ type: 'row', key: `row-${rh.key}`, row: rh });

      for (const depth of selectedDepthsDesc) {
        if (!rh.labels || rh.labels.length < depth) continue;
        const curPrefix = prefix(rh.labels, depth);
        const nextPrefix = next?.labels ? prefix(next.labels, depth) : null;
        if (curPrefix !== nextPrefix) {
          const st = subtotalByKey.get(prefixKey(rh.labels, depth));
          if (st) out.push({ type: 'subtotal', key: `subtotal-${st.key}`, subtotal: st });
        }
      }
    }
    return out;
  }, [result, visibleSubtotals, subtotalDepthSet]);

  useEffect(() => {
    setPivotLoadAll(false);
  }, [rows, columns, values, valuesForPivot, debouncedFilters]);

  const measurePivotTableLayout = useCallback(() => {
    const el = tableWrapRef.current;
    if (!el) return;
    const w = Math.max(320, Math.floor(el.clientWidth || 0));
    setListWrapWidth(w);
  }, []);

  useLayoutEffect(() => {
    if (!result) return undefined;
    const tableEl = tableWrapRef.current;
    const sectionEl = pivotSectionRef.current;
    measurePivotTableLayout();
    const ro = new ResizeObserver(() => measurePivotTableLayout());
    if (tableEl) ro.observe(tableEl);
    if (sectionEl) ro.observe(sectionEl);
    window.addEventListener('resize', measurePivotTableLayout);
    return () => {
      ro.disconnect();
      window.removeEventListener('resize', measurePivotTableLayout);
    };
  }, [result, toolsCollapsed, measurePivotTableLayout]);

  /** Sidebar uses `transition-[margin]`; remeasure after animation so columns/rows reflow. */
  useEffect(() => {
    if (!result) return undefined;
    const t = window.setTimeout(measurePivotTableLayout, 340);
    return () => window.clearTimeout(t);
  }, [toolsCollapsed, result, measurePivotTableLayout]);

  useEffect(() => {
    const section = pivotSectionRef.current;
    if (!section) return undefined;
    const onTransitionEnd = (e) => {
      if (e.target !== section) return;
      if (!String(e.propertyName || '').includes('width')) return;
      measurePivotTableLayout();
    };
    section.addEventListener('transitionend', onTransitionEnd);
    return () => section.removeEventListener('transitionend', onTransitionEnd);
  }, [result, measurePivotTableLayout]);

  useEffect(() => {
    setSubtotalFields((prev) => prev.filter((f) => rows.includes(f)));
  }, [rows]);

  useLayoutEffect(() => {
    if (!result) return undefined;
    const node = pivotListWrapRef.current;
    if (!node) return undefined;
    const ro = new ResizeObserver((entries) => {
      const h = entries[0]?.contentRect?.height ?? 0;
      setPivotListViewportPx(Math.max(80, Math.floor(h)));
    });
    ro.observe(node);
    return () => ro.disconnect();
  }, [result, toolsCollapsed, pivotLayout.headerDepth, orderedBodyRows.length, pivotLoadAll]);

  useEffect(() => {
    try {
      localStorage.setItem(PIVOT_CONFIG_KEY, JSON.stringify({
        rows,
        columns,
        values,
        filters: filtersToPayloadArray(filters),
        subtotalFields,
      }));
    } catch {
      // Ignore storage write errors.
    }
  }, [rows, columns, values, filters, subtotalFields]);

  useEffect(() => {
    // Keep filter dropdown enterprise-safe: do not allow ID field filters.
    setFilters((prev) => prev.filter((f) => f?.field !== 'id'));
  }, [fields]);

  useEffect(() => {
    const img = new Image();
    img.src = 'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7';
    dragGhostRef.current = img;
  }, []);

  useEffect(() => {
    if (!fields.length) return;
    const validFieldSet = new Set(fields.map((f) => f.field));

    setRows((prev) => prev.filter((f) => validFieldSet.has(f)));
    setColumns((prev) => prev.filter((f) => validFieldSet.has(f)));
    setValues((prev) => prev.filter((v) => validFieldSet.has(v?.field)));
    setFilters((prev) => prev.filter((f) => validFieldSet.has(f?.field)));
    setSubtotalFields((prev) => prev.filter((f) => validFieldSet.has(f)));
  }, [fields]);

  useEffect(() => {
    let cancelled = false;
    setLoadingFields(true);
    setError(null);
    pivotApi.fields()
      .then(({ data }) => {
        if (cancelled) return;
        setFields(data?.fields || []);
      })
      .catch((e) => {
        if (cancelled) return;
        setError(parseErr(e, 'Failed to load pivot fields'));
      })
      .finally(() => {
        if (!cancelled) setLoadingFields(false);
      });
    return () => { cancelled = true; };
  }, []);

  const runPivot = useCallback(async (force = false) => {
    const subtotalKey = [...subtotalFields].sort().join('\0');
    const runKey = JSON.stringify({
      rows,
      columns,
      values: valuesForPivot,
      filtersArr: debouncedFiltersPayload,
      sort: DEFAULT_PIVOT_SORT,
      pivotLoadAll,
      subtotalKey,
    });
    if (!force && runKey === lastRunKeyRef.current) return;
    const cacheHit = pivotResponseCacheRef.current.get(runKey);
    if (!force && cacheHit && Date.now() - cacheHit.ts < PIVOT_CLIENT_CACHE_TTL_MS) {
      setResult(cacheHit.data);
      lastRunKeyRef.current = runKey;
      return;
    }
    if (loadMoreAbortRef.current) {
      loadMoreAbortRef.current.abort();
      loadMoreAbortRef.current = null;
    }
    pivotLoadMoreInflightRef.current = false;
    const seq = ++runSeqRef.current;
    if (pivotRequestAbortRef.current) {
      pivotRequestAbortRef.current.abort();
    }
    const controller = new AbortController();
    pivotRequestAbortRef.current = controller;
    setRunning(true);
    setError(null);
    try {
      const payload = {
        rows,
        columns,
        values: valuesForPivot,
        filters: debouncedFiltersPayload,
        sort: DEFAULT_PIVOT_SORT,
        limitRows: MAX_SALES_ROWS,
        subtotalFields,
        ...(pivotLoadAll
          ? {}
          : { bodyOffset: 0, bodyLimit: PIVOT_BODY_PAGE_SIZE }),
      };
      const { data } = await pivotApi.run(payload, { signal: controller.signal });
      // Ignore stale responses when user changes config rapidly.
      if (seq !== runSeqRef.current) return;
      setResult(data);
      pivotResponseCacheRef.current.set(runKey, { ts: Date.now(), data });
      if (pivotResponseCacheRef.current.size > 30) {
        const oldestKey = pivotResponseCacheRef.current.keys().next().value;
        pivotResponseCacheRef.current.delete(oldestKey);
      }
      lastRunKeyRef.current = runKey;
    } catch (e) {
      if (e?.code === 'ERR_CANCELED' || e?.name === 'CanceledError') return;
      if (seq !== runSeqRef.current) return;
      setError(parseErr(e, 'Failed to run pivot', LONG_RUNNING_REQUEST_MS));
    } finally {
      if (seq === runSeqRef.current) setRunning(false);
      if (pivotRequestAbortRef.current === controller) {
        pivotRequestAbortRef.current = null;
      }
    }
  }, [rows, columns, valuesForPivot, debouncedFiltersPayload, subtotalFields, pivotLoadAll]);

  const loadMorePivotBody = useCallback(async () => {
    if (pivotLoadAll) return;
    const bp = result?.meta?.bodyPaging;
    if (!bp?.truncatedAfter) return;
    const loaded = result?.bodyLines?.length ?? 0;
    if (loaded >= (bp.totalLines ?? 0)) return;
    if (pivotLoadMoreInflightRef.current || running) return;

    const seqAtStart = runSeqRef.current;
    const subtotalKey = [...subtotalFields].sort().join('\0');
    const runKey = JSON.stringify({
      rows,
      columns,
      values: valuesForPivot,
      filtersArr: debouncedFiltersPayload,
      sort: DEFAULT_PIVOT_SORT,
      pivotLoadAll: false,
      subtotalKey,
    });
    if (runKey !== lastRunKeyRef.current) return;

    pivotLoadMoreInflightRef.current = true;
    const c = new AbortController();
    loadMoreAbortRef.current = c;
    setBodyLoadingMore(true);
    try {
      const payload = {
        rows,
        columns,
        values: valuesForPivot,
        filters: debouncedFiltersPayload,
        sort: DEFAULT_PIVOT_SORT,
        limitRows: MAX_SALES_ROWS,
        subtotalFields,
        bodyOffset: loaded,
        bodyLimit: PIVOT_BODY_PAGE_SIZE,
      };
      const { data } = await pivotApi.run(payload, { signal: c.signal });
      if (seqAtStart !== runSeqRef.current) return;
      if (!Array.isArray(data?.bodyLines) || data.bodyLines.length === 0) {
        setResult((prev) => {
          if (!prev?.meta?.bodyPaging) return prev;
          return {
            ...prev,
            meta: {
              ...prev.meta,
              bodyPaging: { ...prev.meta.bodyPaging, truncatedAfter: false },
            },
          };
        });
        return;
      }
      setResult((prev) => {
        const merged = mergePivotBodyPages(prev, data, PIVOT_BODY_PAGE_SIZE);
        pivotResponseCacheRef.current.set(runKey, { ts: Date.now(), data: merged });
        return merged;
      });
    } catch (e) {
      if (e?.code === 'ERR_CANCELED' || e?.name === 'CanceledError') return;
      if (seqAtStart !== runSeqRef.current) return;
      setError(parseErr(e, 'Failed to load more pivot rows', LONG_RUNNING_REQUEST_MS));
    } finally {
      pivotLoadMoreInflightRef.current = false;
      if (loadMoreAbortRef.current === c) loadMoreAbortRef.current = null;
      setBodyLoadingMore(false);
    }
  }, [
    pivotLoadAll,
    result,
    running,
    rows,
    columns,
    valuesForPivot,
    debouncedFiltersPayload,
    subtotalFields,
  ]);

  useEffect(() => () => {
    if (pivotRequestAbortRef.current) pivotRequestAbortRef.current.abort();
  }, []);

  useEffect(() => {
    if (loadingFields) return undefined;
    if (valuesForPivot.length === 0) {
      // Invalidate any in-flight pivot request so a late response cannot repopulate `result`
      // after the user cleared all rows/columns/values (and implicit default measures).
      runSeqRef.current += 1;
      if (loadMoreAbortRef.current) {
        loadMoreAbortRef.current.abort();
        loadMoreAbortRef.current = null;
      }
      pivotLoadMoreInflightRef.current = false;
      if (pivotRequestAbortRef.current) {
        pivotRequestAbortRef.current.abort();
        pivotRequestAbortRef.current = null;
      }
      setRunning(false);
      setResult(null);
      setError(null);
      lastRunKeyRef.current = '';
      return undefined;
    }
    const invalidFilterMessage = getInvalidFilterMessage(debouncedFilters);
    if (invalidFilterMessage) {
      setResult(null);
      lastRunKeyRef.current = '';
      setError(invalidFilterMessage);
      return undefined;
    }
    runPivot();
    return undefined;
  }, [rows, columns, valuesForPivot, debouncedFilters, subtotalFields, loadingFields, runPivot]);

  const addFilter = (field) => {
    if (!field) return;
    setFilters((x) => [...x, { field, operator: 'in', values: [], value: '' }]);
  };

  const toggleFilterMultiValue = (filterIndex, optionValue) => {
    setFilters((arr) => arr.map((item, i) => {
      if (i !== filterIndex) return item;
      const prevVals = Array.isArray(item.values) ? item.values : (item.value ? [item.value] : []);
      const exists = prevVals.includes(optionValue);
      const nextVals = exists
        ? prevVals.filter((v) => v !== optionValue)
        : [...prevVals, optionValue];
      return {
        ...item,
        operator: 'in',
        values: nextVals,
        value: nextVals[0] || '',
      };
    }));
  };

  const moveFilter = (fromIndex, toIndex) => {
    setFilters((arr) => {
      if (toIndex < 0 || toIndex >= arr.length) return arr;
      const next = [...arr];
      const [item] = next.splice(fromIndex, 1);
      next.splice(toIndex, 0, item);
      return next;
    });
  };

  const moveRow = (fromIndex, toIndex) => {
    setRows((arr) => {
      if (fromIndex < 0 || fromIndex >= arr.length || fromIndex === toIndex) return arr;
      const next = [...arr];
      const [item] = next.splice(fromIndex, 1);
      next.splice(toIndex, 0, item);
      return next;
    });
  };

  const moveColumn = (fromIndex, toIndex) => {
    setColumns((arr) => {
      if (fromIndex < 0 || fromIndex >= arr.length || fromIndex === toIndex) return arr;
      const next = [...arr];
      const [item] = next.splice(fromIndex, 1);
      next.splice(toIndex, 0, item);
      return next;
    });
  };

  const handleFilterReorderDrop = (event, targetIndex) => {
    event.preventDefault();
    event.stopPropagation();
    const payload = parseDragPayload(event) || dragItem;
    if (!payload || payload.source !== 'filters') return;
    const fromIndex = Number(payload.filterIndex);
    if (!Number.isInteger(fromIndex) || fromIndex === targetIndex) return;
    moveFilter(fromIndex, targetIndex);
  };

  /** Drop on a filter card: reorder filters, or accept a field from another zone (pool / rows / …). */
  const handleFilterCardDrop = (event, targetIndex) => {
    const payload = parseDragPayload(event) || dragItem;
    if (payload?.source === 'filters') {
      handleFilterReorderDrop(event, targetIndex);
      return;
    }
    handleDropToZone(event, 'filters');
  };

  const handleRowDrop = (event, targetIndex) => {
    event.preventDefault();
    event.stopPropagation();
    setDropTarget('');
    const payload = parseDragPayload(event) || dragItem;
    if (!payload?.field || fieldByName[payload.field]?.group !== 'dimension') return;

    if (payload.source === 'rows') {
      const fromIndex = rows.indexOf(payload.field);
      if (fromIndex === -1 || fromIndex === targetIndex) return;
      moveRow(fromIndex, targetIndex);
      return;
    }

    if (payload.source === 'columns') setColumns((prev) => prev.filter((v) => v !== payload.field));
    if (payload.source === 'filters') {
      const fIdx = Number(payload.filterIndex);
      setFilters((prev) => (
        Number.isInteger(fIdx)
          ? prev.filter((_, i) => i !== fIdx)
          : prev.filter((v) => v.field !== payload.field)
      ));
    }
    if (payload.source === 'values') setValues((prev) => prev.filter((v) => v.field !== payload.field));

    setRows((prev) => {
      const without = prev.filter((v) => v !== payload.field);
      const pos = Math.min(Math.max(0, targetIndex), without.length);
      const next = [...without];
      next.splice(pos, 0, payload.field);
      return next;
    });
  };

  const handleColumnDrop = (event, targetIndex) => {
    event.preventDefault();
    event.stopPropagation();
    setDropTarget('');
    const payload = parseDragPayload(event) || dragItem;
    if (!payload?.field || fieldByName[payload.field]?.group !== 'dimension') return;

    if (payload.source === 'columns') {
      const fromIndex = columns.indexOf(payload.field);
      if (fromIndex === -1 || fromIndex === targetIndex) return;
      moveColumn(fromIndex, targetIndex);
      return;
    }

    if (payload.source === 'rows') setRows((prev) => prev.filter((v) => v !== payload.field));
    if (payload.source === 'filters') {
      const fIdx = Number(payload.filterIndex);
      setFilters((prev) => (
        Number.isInteger(fIdx)
          ? prev.filter((_, i) => i !== fIdx)
          : prev.filter((v) => v.field !== payload.field)
      ));
    }
    if (payload.source === 'values') setValues((prev) => prev.filter((v) => v.field !== payload.field));

    setColumns((prev) => {
      const without = prev.filter((v) => v !== payload.field);
      const pos = Math.min(Math.max(0, targetIndex), without.length);
      const next = [...without];
      next.splice(pos, 0, payload.field);
      return next;
    });
  };

  const handleValueDrop = (event, targetIndex) => {
    event.preventDefault();
    event.stopPropagation();
    setDropTarget('');
    const payload = parseDragPayload(event) || dragItem;
    if (!payload?.field || fieldByName[payload.field]?.group !== 'measure') return;

    if (payload.source === 'values') {
      const fromIdx = Number(payload.valueIndex);
      if (!Number.isInteger(fromIdx) || fromIdx === targetIndex) return;
      setValues((arr) => {
        if (fromIdx < 0 || fromIdx >= arr.length) return arr;
        const next = [...arr];
        const [item] = next.splice(fromIdx, 1);
        next.splice(targetIndex, 0, item);
        return next;
      });
      return;
    }

    if (payload.source === 'rows') setRows((prev) => prev.filter((v) => v !== payload.field));
    if (payload.source === 'columns') setColumns((prev) => prev.filter((v) => v !== payload.field));
    if (payload.source === 'filters') {
      const fIdx = Number(payload.filterIndex);
      setFilters((prev) => (
        Number.isInteger(fIdx)
          ? prev.filter((_, i) => i !== fIdx)
          : prev.filter((v) => v.field !== payload.field)
      ));
    }

    const defaultAgg = fieldByName[payload.field]?.defaultAgg || payload.agg || 'sum';
    setValues((prev) => {
      const without = prev.filter((v) => v.field !== payload.field);
      const pos = Math.min(Math.max(0, targetIndex), without.length);
      const next = [...without];
      const agg = String(defaultAgg).toLowerCase();
      next.splice(pos, 0, {
        field: payload.field,
        agg,
        label: `${agg.toUpperCase()} ${payload.field}`,
      });
      return next;
    });
  };

  const moveDimensionField = (target, field) => {
    if (!field || field === 'id') return;
    if (fieldByName[field]?.group !== 'dimension') return;
    setRows((prev) => prev.filter((v) => v !== field));
    setColumns((prev) => prev.filter((v) => v !== field));
    if (target === 'rows') setRows((prev) => (prev.includes(field) ? prev : [...prev, field]));
    if (target === 'columns') setColumns((prev) => (prev.includes(field) ? prev : [...prev, field]));
  };

  const moveValueField = (field, aggFromDrag) => {
    if (!field || field === 'id') return;
    const defaultAgg = fieldByName[field]?.defaultAgg || aggFromDrag || 'sum';
    setValues((prev) => (
      prev.some((v) => v.field === field)
        ? prev
        : [...prev, { field, agg: defaultAgg, label: `${defaultAgg.toUpperCase()} ${field}` }]
    ));
  };

  /** Excel-style: field is “in the report” if it appears in any area. */
  const isPivotFieldActive = (fieldName) => {
    if (!fieldName || fieldName === 'id') return false;
    return (
      filters.some((x) => x.field === fieldName)
      || rows.includes(fieldName)
      || columns.includes(fieldName)
      || values.some((v) => v.field === fieldName)
    );
  };

  /** Checkbox on / off mirrors Excel field list (measure → Values, dimension → Rows). */
  const togglePivotFieldActive = (fieldName, checked) => {
    if (!fieldName || fieldName === 'id') return;
    if (!checked) {
      setFilters((arr) => arr.filter((x) => x.field !== fieldName));
      setRows((arr) => arr.filter((x) => x !== fieldName));
      setColumns((arr) => arr.filter((x) => x !== fieldName));
      setValues((arr) => arr.filter((x) => x.field !== fieldName));
      setSubtotalFields((arr) => arr.filter((x) => x !== fieldName));
      return;
    }
    const meta = fieldByName[fieldName];
    if (!meta) return;
    if (meta.group === 'measure') moveValueField(fieldName, null);
    else moveDimensionField('rows', fieldName);
  };

  const parseDragPayload = (event) => {
    try {
      const raw = event.dataTransfer.getData('application/json');
      if (!raw) return null;
      return JSON.parse(raw);
    } catch {
      return null;
    }
  };

  const handleDropToZone = (event, zone) => {
    event.preventDefault();
    const payload = parseDragPayload(event) || dragItem;
    setDropTarget('');
    if (!payload?.field) return;
    const source = payload.source;

    if (source === 'rows' && zone !== 'rows') setRows((prev) => prev.filter((v) => v !== payload.field));
    if (source === 'columns' && zone !== 'columns') setColumns((prev) => prev.filter((v) => v !== payload.field));
    if (source === 'filters' && zone !== 'filters') {
      const fIdx = Number(payload.filterIndex);
      setFilters((prev) => (
        Number.isInteger(fIdx)
          ? prev.filter((_, i) => i !== fIdx)
          : prev.filter((v) => v.field !== payload.field)
      ));
    }
    if (source === 'values' && zone !== 'values') setValues((prev) => prev.filter((v) => v.field !== payload.field));

    if (zone === 'rows' || zone === 'columns') {
      moveDimensionField(zone, payload.field);
      return;
    }

    if (zone === 'filters') {
      addFilter(payload.field);
      return;
    }

    if (zone === 'values') {
      moveValueField(payload.field, payload.agg);
    }
  };

  /** Drag back to the field list to remove from the report (Excel-style). */
  const handleDropToPool = (event) => {
    event.preventDefault();
    event.stopPropagation();
    const payload = parseDragPayload(event) || dragItem;
    setDropTarget('');
    if (!payload?.field || payload.source === 'pool') return;

    if (payload.source === 'rows') setRows((prev) => prev.filter((v) => v !== payload.field));
    if (payload.source === 'columns') setColumns((prev) => prev.filter((v) => v !== payload.field));
    if (payload.source === 'values') setValues((prev) => prev.filter((v) => v.field !== payload.field));
    if (payload.source === 'filters') {
      const idx = payload.filterIndex;
      setFilters((prev) => {
        if (Number.isInteger(idx)) return prev.filter((_, i) => i !== idx);
        return prev.filter((x) => x.field !== payload.field);
      });
    }
  };

  const handleDragStart = (event, payload) => {
    setDragItem(payload);
    event.dataTransfer.setData('application/json', JSON.stringify(payload));
    event.dataTransfer.effectAllowed = 'move';
    if (dragGhostRef.current) {
      try {
        event.dataTransfer.setDragImage(dragGhostRef.current, 0, 0);
      } catch {
        /* ignore */
      }
    }
  };

  const handleDragEnd = () => {
    setDragItem(null);
    setDropTarget('');
  };

  const handleDragOverZone = (event, zone) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
    setDropTarget((prev) => (prev === zone ? prev : zone));
  };

  /** Avoid clearing highlight when the pointer moves between children inside the same drop zone. */
  const onPivotZoneDragLeave = useCallback((e) => {
    const zone = e.currentTarget.getAttribute('data-pivot-drop-zone');
    if (zone == null) return;
    const related = e.relatedTarget;
    if (related instanceof Node && e.currentTarget.contains(related)) return;
    setDropTarget((prev) => (prev === zone ? '' : prev));
  }, []);

  const handlePivotZoneDragEnter = useCallback((e) => {
    e.preventDefault();
  }, []);

  const loadFilterValues = useCallback(async (field, { force = false } = {}) => {
    if (!field) return;
    if (force) filterFetchedRef.current.delete(field);
    else if (filterFetchedRef.current.has(field)) return;

    if (!force) {
      const peeked = peekPivotFilterValuesCache(field, '', FILTER_VALUES_LIMIT);
      if (peeked && Array.isArray(peeked.values)) {
        setFilterOptions((prev) => ({ ...prev, [field]: peeked.values }));
        filterFetchedRef.current.add(field);
        setFilterReady((prev) => ({ ...prev, [field]: true }));
        setFilterErrorByField((prev) => ({ ...prev, [field]: null }));
        setFilterLoading((prev) => ({ ...prev, [field]: false }));
        return;
      }
    }

    let slowSpinnerTimer = null;
    slowSpinnerTimer = window.setTimeout(() => {
      setFilterLoading((prev) => ({ ...prev, [field]: true }));
    }, 100);

    setFilterReady((prev) => ({ ...prev, [field]: false }));
    setFilterErrorByField((prev) => ({ ...prev, [field]: null }));
    try {
      const { data } = await pivotApi.filterValues({ field, limit: FILTER_VALUES_LIMIT });
      setFilterOptions((prev) => ({ ...prev, [field]: Array.isArray(data?.values) ? data.values : [] }));
      filterFetchedRef.current.add(field);
    } catch (e) {
      setFilterOptions((prev) => ({ ...prev, [field]: [] }));
      setFilterErrorByField((prev) => ({
        ...prev,
        [field]: parseErr(e, 'Failed to load filter values', PIVOT_FILTER_VALUES_TIMEOUT_MS),
      }));
      filterFetchedRef.current.add(field);
    } finally {
      if (slowSpinnerTimer) window.clearTimeout(slowSpinnerTimer);
      setFilterLoading((prev) => ({ ...prev, [field]: false }));
      setFilterReady((prev) => ({ ...prev, [field]: true }));
    }
  }, []);

  useEffect(() => {
    const active = new Set(filters.map((x) => x?.field).filter(Boolean));
    for (const key of [...filterFetchedRef.current]) {
      if (!active.has(key)) filterFetchedRef.current.delete(key);
    }
    setFilterOptions((prev) => {
      const next = { ...prev };
      for (const k of Object.keys(next)) {
        if (!active.has(k)) delete next[k];
      }
      return next;
    });
    setFilterErrorByField((prev) => {
      const next = { ...prev };
      for (const k of Object.keys(next)) {
        if (!active.has(k)) delete next[k];
      }
      return next;
    });
    setFilterLoading((prev) => {
      const next = { ...prev };
      for (const k of Object.keys(next)) {
        if (!active.has(k)) delete next[k];
      }
      return next;
    });
    setFilterReady((prev) => {
      const next = { ...prev };
      for (const k of Object.keys(next)) {
        if (!active.has(k)) delete next[k];
      }
      return next;
    });
  }, [filters]);

  useEffect(() => {
    const uniqueFields = [...new Set(filters.map((x) => x?.field).filter(Boolean))];
    const fromCache = {};
    const toNetwork = [];
    for (const f of uniqueFields) {
      if (filterFetchedRef.current.has(f)) continue;
      const peeked = peekPivotFilterValuesCache(f, '', FILTER_VALUES_LIMIT);
      if (peeked && Array.isArray(peeked.values)) {
        fromCache[f] = peeked.values;
        filterFetchedRef.current.add(f);
      } else {
        toNetwork.push(f);
      }
    }
    if (Object.keys(fromCache).length > 0) {
      const keys = Object.keys(fromCache);
      setFilterOptions((prev) => ({ ...prev, ...fromCache }));
      setFilterReady((prev) => {
        const next = { ...prev };
        for (const k of keys) next[k] = true;
        return next;
      });
      setFilterErrorByField((prev) => {
        const next = { ...prev };
        for (const k of keys) next[k] = null;
        return next;
      });
      setFilterLoading((prev) => {
        const next = { ...prev };
        for (const k of keys) next[k] = false;
        return next;
      });
    }
    if (toNetwork.length === 0) return;

    const gen = (filterBatchGenRef.current += 1);
    const slowSpinnerTimer = window.setTimeout(() => {
      if (gen !== filterBatchGenRef.current) return;
      setFilterLoading((prev) => {
        const next = { ...prev };
        for (const f of toNetwork) next[f] = true;
        return next;
      });
    }, 100);

    for (const f of toNetwork) {
      setFilterReady((prev) => ({ ...prev, [f]: false }));
      setFilterErrorByField((prev) => ({ ...prev, [f]: null }));
    }

    (async () => {
      try {
        const { data } = await pivotApi.filterValuesBatch({ fields: toNetwork, limit: FILTER_VALUES_LIMIT });
        if (gen !== filterBatchGenRef.current) return;
        const bag = data?.fields && typeof data.fields === 'object' ? data.fields : {};
        setFilterOptions((prev) => {
          const next = { ...prev };
          for (const f of toNetwork) {
            const entry = bag[f];
            next[f] = entry?.error ? [] : Array.isArray(entry?.values) ? entry.values : [];
          }
          return next;
        });
        setFilterErrorByField((prev) => {
          const next = { ...prev };
          for (const f of toNetwork) {
            const entry = bag[f];
            next[f] = entry?.error ? String(entry.error) : null;
          }
          return next;
        });
        for (const f of toNetwork) filterFetchedRef.current.add(f);
      } catch (e) {
        if (gen !== filterBatchGenRef.current) return;
        const msg = parseErr(e, 'Failed to load filter values', PIVOT_FILTER_VALUES_TIMEOUT_MS);
        setFilterOptions((prev) => {
          const next = { ...prev };
          for (const f of toNetwork) next[f] = [];
          return next;
        });
        setFilterErrorByField((prev) => {
          const next = { ...prev };
          for (const f of toNetwork) next[f] = msg;
          return next;
        });
        for (const f of toNetwork) filterFetchedRef.current.add(f);
      } finally {
        window.clearTimeout(slowSpinnerTimer);
        if (gen !== filterBatchGenRef.current) return;
        setFilterLoading((prev) => {
          const next = { ...prev };
          for (const f of toNetwork) next[f] = false;
          return next;
        });
        setFilterReady((prev) => {
          const next = { ...prev };
          for (const f of toNetwork) next[f] = true;
          return next;
        });
      }
    })();
  }, [filters]);

  const descriptorHeader = useMemo(() => (
    pivotLayout.descriptors.map((d) => {
      const metric = metricHeaderLabel(d.metricKey);
      if (d.kind === 'total') return `GRAND TOTAL | ${metric}`;
      const axis = d.labels.filter(Boolean).join(' / ') || '(all)';
      return `${axis} | ${metric}`;
    })
  ), [pivotLayout.descriptors]);

  const columnWidths = useMemo(() => {
    const rowWidths = rowHeaderFields.map((rf, idx) => (
      widthFromHeaderText(fieldLabel(rf), { min: idx === 0 ? 110 : 96, max: idx === 0 ? 210 : 180, pad: 24 })
    ));
    const valueWidths = descriptorHeader.map((h) => widthFromHeaderText(h, { min: 92, max: 170, pad: 22 }));
    return [...rowWidths, ...valueWidths];
  }, [rowHeaderFields, descriptorHeader]);
  const pivotTableMinWidth = useMemo(
    () => columnWidths.reduce((a, w) => a + w, 0),
    [columnWidths],
  );
  const targetTableWidth = Math.max(pivotTableMinWidth, listWrapWidth);
  const layoutColumnWidths = useMemo(
    () => expandColumnWidthsToTarget(columnWidths, targetTableWidth),
    [columnWidths, targetTableWidth],
  );
  const pivotOuterWidth = layoutColumnWidths.reduce((a, w) => a + w, 0);
  /** Use exact pixel tracks so body grid lines align 1:1 with header/footer colgroup widths. */
  const gridTemplateColumns = useMemo(
    () => layoutColumnWidths.map((w) => `${w}px`).join(' '),
    [layoutColumnWidths],
  );

  const pivotScrollRef = useRef(null);

  const rowVirtualizer = useVirtualizer({
    count: orderedBodyRows.length,
    getScrollElement: () => pivotScrollRef.current,
    estimateSize: () => PIVOT_BODY_ROW_HEIGHT,
    overscan: 10,
  });

  const listViewportHeight = useMemo(() => {
    if (orderedBodyRows.length === 0) return 0;
    return Math.max(80, pivotListViewportPx);
  }, [orderedBodyRows.length, pivotListViewportPx]);

  const onPivotVirtualScroll = useCallback(() => {
    if (pivotLoadAll) return;
    if (!result?.meta?.bodyPaging?.truncatedAfter) return;
    const n = orderedBodyRows.length;
    if (n === 0) return;
    const vis = rowVirtualizer.getVirtualItems();
    if (!vis.length) return;
    if (vis[vis.length - 1].index >= n - 8) void loadMorePivotBody();
  }, [
    pivotLoadAll,
    result?.meta?.bodyPaging?.truncatedAfter,
    orderedBodyRows.length,
    rowVirtualizer,
    loadMorePivotBody,
  ]);

  const buildVisibleGrid = useCallback(() => {
    if (!result) return [];
    const header = [...rowHeaderFields.map((f) => fieldLabel(f)), ...descriptorHeader];
    const rowsOut = [header];
    for (const entry of orderedBodyRows) {
      if (entry.type === 'row') {
        const rh = entry.row;
        const left = rowHeaderFields.map((_, idx) => String(rh.labels?.[idx] ?? (idx === 0 ? '(all)' : '')));
        const right = pivotLayout.descriptors.map((d) => {
          const raw = d.kind === 'total'
            ? result.rowTotals?.[rh.key]?.[d.metricKey]
            : result.cells?.[rh.key]?.[d.colKey]?.[d.metricKey];
          return formatPivotCellValue(raw, formatPivotIndian, d.metricKey);
        });
        rowsOut.push([...left, ...right]);
      } else {
        const st = entry.subtotal;
        const left = rowHeaderFields.map((_, idx) => String(st.labels?.[idx] ?? (idx === 0 ? 'SUBTOTAL' : '')));
        const right = pivotLayout.descriptors.map((d) => {
          if (d.kind === 'total') {
            const n = subtotalRowTotalNumeric(st, d.metricKey);
            return formatPivotCellValue(n, formatPivotIndian, d.metricKey);
          }
          return formatPivotCellValue(st.cells?.[d.colKey]?.[d.metricKey], formatPivotIndian, d.metricKey);
        });
        rowsOut.push([...left, ...right]);
      }
    }
    const grandLeft = rowHeaderFields.map((_, idx) => (idx === 0 ? 'GRAND TOTAL' : ''));
    const grandRight = pivotLayout.descriptors.map((d) => {
      const raw = d.kind === 'total'
        ? result.grandTotals?.[d.metricKey]
        : result.columnTotals?.[d.colKey]?.[d.metricKey];
      return formatPivotCellValue(raw, formatPivotIndian, d.metricKey);
    });
    rowsOut.push([...grandLeft, ...grandRight]);
    return rowsOut;
  }, [result, rowHeaderFields, descriptorHeader, orderedBodyRows, pivotLayout.descriptors]);

  const grandFooterRowIndex = 1 + orderedBodyRows.length;

  const getRange = () => {
    if (!selectionStart || !selectionEnd) return null;
    return {
      r1: Math.min(selectionStart.r, selectionEnd.r),
      r2: Math.max(selectionStart.r, selectionEnd.r),
      c1: Math.min(selectionStart.c, selectionEnd.c),
      c2: Math.max(selectionStart.c, selectionEnd.c),
    };
  };

  const selectAllPivotGrid = useCallback(() => {
    const full = buildVisibleGrid();
    if (!full?.length) return;
    const lastRow = full.length - 1;
    const lastCol = Math.max(0, (full[0]?.length || 1) - 1);
    setSelectionStart({ r: 0, c: 0 });
    setSelectionEnd({ r: lastRow, c: lastCol });
    setCopyStatus('Selected entire pivot grid.');
  }, [buildVisibleGrid]);

  const selectedGrid = useMemo(() => {
    const full = buildVisibleGrid();
    if (!selectionStart && !selectionEnd) return full;
    if (selectionStart && !selectionEnd) {
      const one = full?.[selectionStart.r]?.[selectionStart.c];
      return [[one == null ? '' : one]];
    }
    const range = getRange();
    if (!range || !full.length) return full;
    return full
      .slice(range.r1, range.r2 + 1)
      .map((row) => row.slice(range.c1, range.c2 + 1));
  }, [buildVisibleGrid, selectionStart, selectionEnd]);

  const isSelectedCell = useCallback((r, c) => {
    const range = getRange();
    if (!range) return false;
    return r >= range.r1 && r <= range.r2 && c >= range.c1 && c <= range.c2;
  }, [selectionStart, selectionEnd]);

  const onCellClick = useCallback((r, c) => {
    if (!selectionStart || (selectionStart && selectionEnd)) {
      setSelectionStart({ r, c });
      setSelectionEnd(null);
      return;
    }
    setSelectionEnd({ r, c });
  }, [selectionStart, selectionEnd]);

  useEffect(() => {
    pivotGridRef.current = tableWrapRef.current;
  }, [result]);

  useEffect(() => {
    const node = pivotGridRef.current;
    if (!node) return undefined;
    const onKeyDown = (e) => {
      const key = String(e.key || '').toLowerCase();
      if ((e.ctrlKey || e.metaKey) && key === 'a') {
        e.preventDefault();
        selectAllPivotGrid();
      }
    };
    node.addEventListener('keydown', onKeyDown);
    return () => node.removeEventListener('keydown', onKeyDown);
  }, [selectAllPivotGrid]);

  const pivotListRowProps = useMemo(
    () => ({
      orderedBodyRows,
      rowHeaderFields,
      descriptors: pivotLayout.descriptors,
      rowTotals: result?.rowTotals,
      cells: result?.cells,
      formatPivotIndian,
      isSelectedCell,
      onCellClick,
      gridTemplateColumns,
    }),
    [
      orderedBodyRows,
      rowHeaderFields,
      pivotLayout.descriptors,
      result?.rowTotals,
      result?.cells,
      isSelectedCell,
      onCellClick,
      gridTemplateColumns,
    ],
  );

  useEffect(() => {
    if (!copyStatus) return undefined;
    const t = setTimeout(() => setCopyStatus(''), 2200);
    return () => clearTimeout(t);
  }, [copyStatus]);

  const downloadPivotExcel = useCallback(async () => {
    if (!result || running || exportingPivot) return;

    const exportSelectionOnly = Boolean(selectionStart && selectionEnd);
    if (exportSelectionOnly) {
      const grid = selectedGrid;
      if (!grid?.length) return;
      try {
        const csv = grid
          .map((r) =>
            r.map((c) => `"${String(c ?? '').replace(/"/g, '""')}"`).join(','),
          )
          .join('\n');
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        const stamp = new Date().toISOString().slice(0, 10);
        a.download = `pivot_report_${stamp}.csv`;
        a.click();
        URL.revokeObjectURL(url);
        setCopyStatus('Selected range downloaded as CSV.');
      } catch (e) {
        setError(e?.message || 'Failed to export CSV.');
      }
      return;
    }

    const invalidFilterMessage = getInvalidFilterMessage(filters);
    if (invalidFilterMessage) {
      setCopyStatus(invalidFilterMessage);
      return;
    }

    setExportingPivot(true);
    setCopyStatus('');
    try {
      const config = {
        rows,
        columns,
        values: valuesForPivot,
        filters: filtersToPayloadArray(filters),
        sort: DEFAULT_PIVOT_SORT,
        limitRows: MAX_SALES_ROWS,
        subtotalFields,
      };
      const res = await pivotApi.export({ format: 'xlsx', config });
      const ctype = String(res.headers?.['content-type'] || '');
      if (ctype.includes('application/json')) {
        const text = await res.data.text();
        let msg = text;
        try {
          msg = JSON.parse(text).error || text;
        } catch {
          /* keep */
        }
        throw new Error(msg);
      }
      const blob = new Blob([res.data], {
        type: ctype || 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      });
      const stamp = new Date().toISOString().slice(0, 10);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `pivot_report_${stamp}.xlsx`;
      a.rel = 'noopener';
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
      setCopyStatus('Full pivot report downloaded (all body rows).');
    } catch (e) {
      const data = e?.response?.data;
      if (data instanceof Blob) {
        try {
          const text = await data.text();
          const j = JSON.parse(text);
          setError(j.error || text || 'Export failed');
        } catch {
          setError('Export failed');
        }
      } else {
        setError(parseErr(e, 'Failed to export pivot', LONG_RUNNING_REQUEST_MS));
      }
    } finally {
      setExportingPivot(false);
    }
  }, [
    result,
    running,
    exportingPivot,
    selectedGrid,
    selectionStart,
    selectionEnd,
    filters,
    rows,
    columns,
    valuesForPivot,
    subtotalFields,
  ]);

  const hasAnyPivotField = rows.length > 0 || columns.length > 0 || values.length > 0;

  if (loadingFields) {
    return (
      <section className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden" aria-labelledby="report-heading" aria-busy="true">
        <div className="px-6 py-5 border-b border-slate-100 bg-slate-50/50">
          <h2 id="report-heading" className="text-base font-semibold text-slate-800">Pivot Report</h2>
            <p className="text-sm text-slate-500 mt-1">Excel-like pivot with filters, totals and export.</p>
        </div>
        <div className="flex flex-col items-center justify-center gap-3 py-20 px-6" role="status" aria-label="Loading pivot fields">
          <Loader2 className="h-10 w-10 animate-spin text-blue-600" aria-hidden />
          <p className="text-sm font-medium text-slate-600">Loading pivot fields…</p>
        </div>
      </section>
    );
  }

  return (
    <div className="relative">
    <section
      ref={pivotSectionRef}
      className={[
        // Fixed tools panel does not consume flow space; margin-right does NOT shrink w-full, so use
        // explicit width on lg — otherwise tableWrap clientWidth never changes when collapsing tools.
        'w-full min-w-0 max-w-full bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden transition-[width] duration-200 ease-out',
        toolsCollapsed ? 'lg:w-[calc(100%-3.5rem)]' : 'lg:w-[calc(100%-300px)]',
      ].join(' ')}
      aria-labelledby="report-heading"
    >
      <div className="px-6 py-5 border-b border-slate-100 bg-slate-50/50">
        <h2 id="report-heading" className="text-base font-semibold text-slate-800">Pivot Report</h2>
      </div>
      <div className="w-full min-w-0 p-3 sm:p-4">
        <div className="w-full min-w-0 space-y-4">

        {error && <div className="text-sm text-red-700 bg-red-50 border border-red-200 rounded-lg px-4 py-3">{error}</div>}

          {!result && !running && hasAnyPivotField && (
          <div className="rounded-xl border border-dashed border-slate-300 bg-slate-50/40 py-14 px-6 text-center">
            <p className="text-sm font-medium text-slate-700">No report generated yet</p>
            <p className="text-xs text-slate-500 mt-1">Choose fields above and report updates automatically.</p>
          </div>
        )}

        {!result && running && hasAnyPivotField && (
          <div className="rounded-xl border border-slate-200 bg-white py-14 px-6">
            <div className="flex flex-col items-center justify-center gap-2 text-slate-600" role="status" aria-label="Running pivot report">
              <Loader2 className="h-7 w-7 animate-spin text-blue-600" aria-hidden />
              <p className="text-sm font-medium">Updating pivot data...</p>
            </div>
          </div>
        )}

        {result && hasAnyPivotField && (
          <div className="w-full min-w-0 overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm">
            <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-100 bg-slate-50/60 px-4 py-2.5 text-xs text-slate-600">
              <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
                <span><span className="font-medium text-slate-700">Source rows:</span> {formatPivotIndian(result?.meta?.sourceRows ?? 0)}</span>
                <span><span className="font-medium text-slate-700">Filtered rows:</span> {formatPivotIndian(result?.meta?.filteredRows ?? 0)}</span>
                <span><span className="font-medium text-slate-700">Cells:</span> {formatPivotIndian(result?.meta?.visibleCells ?? 0)}</span>
                {typeof result?.meta?.executionMs === 'number' && (
                  <span title="Server-side pivot execution time">
                    <span className="font-medium text-slate-700">Server time:</span>{' '}
                    {result.meta.executionMs.toLocaleString('en-IN')} ms
                  </span>
                )}
                {result?.meta?.engine && (
                  <span title="postgres = SQL aggregation; stream = row scan fallback">
                    <span className="font-medium text-slate-700">Engine:</span> {result.meta.engine}
                  </span>
                )}
              </div>
              <button
                type="button"
                className="inline-flex items-center justify-center gap-1.5 rounded-md border border-emerald-600/40 bg-emerald-50 px-2.5 py-1 text-[11px] font-medium text-emerald-800 hover:bg-emerald-100 disabled:opacity-50"
                onClick={() => void downloadPivotExcel()}
                disabled={running || exportingPivot}
                title="Download full pivot as .xlsx from the server. Select two cells in the grid first to export only that range."
              >
                {exportingPivot ? (
                  <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin" aria-hidden />
                ) : (
                  <FileSpreadsheet className="h-3.5 w-3.5 shrink-0" aria-hidden />
                )}
                {exportingPivot ? 'Preparing…' : 'Download Excel'}
              </button>
            </div>
            {Array.isArray(result?.meta?.warnings) && result.meta.warnings.length > 0 && (
              <div className="border-b border-amber-200 bg-amber-50/90 px-4 py-2 text-[11px] text-amber-950 space-y-1">
                {result.meta.warnings.map((w, i) => (
                  <p key={`pivot-warn-${i}`}>{w}</p>
                ))}
              </div>
            )}
            {filters.length > 0 && (
              <div className="flex flex-wrap items-center gap-2 border-b border-slate-300 bg-[#f2f2f2] px-3 py-2">
                <span className="text-[10px] font-bold uppercase tracking-wide text-slate-800">Report filter</span>
                {filters.map((f, idx) => (
                  <span
                    key={`${f.field}-rf-${idx}`}
                    className="inline-flex max-w-[16rem] items-center gap-1.5 rounded border border-slate-400 bg-white px-2 py-1 text-[10px] text-slate-800 shadow-sm"
                    title={`${fieldLabel(f.field)} — ${summarizePivotFilter(f)}`}
                  >
                    <span className="shrink-0 font-semibold">{fieldLabel(f.field)}</span>
                    <span className="min-w-0 truncate text-slate-600">{summarizePivotFilter(f)}</span>
                  </span>
                ))}
              </div>
            )}
            {result?.meta?.bodyPaging?.truncatedAfter && (
              <div className="flex flex-wrap items-center gap-2 border-b border-amber-200 bg-amber-50/90 px-4 py-2 text-[11px] text-amber-950">
                <span>
                  Loaded {orderedBodyRows.length.toLocaleString('en-IN')} of{' '}
                  {result.meta.bodyPaging.totalLines.toLocaleString('en-IN')} body lines. Grand totals are for the full filtered set.
                  Scroll the pivot body to load {PIVOT_BODY_PAGE_SIZE.toLocaleString('en-IN')} more at a time, or load everything below.
                </span>
                <button
                  type="button"
                  className="rounded-md border border-amber-600/50 bg-white px-2 py-0.5 font-medium hover:bg-amber-100"
                  onClick={() => {
                    setPivotLoadAll(true);
                    runPivot(true);
                  }}
                >
                  Load all body lines
                </button>
              </div>
            )}
            <div
              ref={tableWrapRef}
              tabIndex={0}
              onFocus={() => { pivotGridRef.current = tableWrapRef.current; }}
              className="relative flex min-h-0 w-full min-w-0 max-w-full flex-1 flex-col overflow-hidden focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/50"
              style={{
                height: 'min(75vh, calc(100vh - 7rem))',
                maxHeight: 'min(75vh, calc(100vh - 7rem))',
                minHeight: '12rem',
              }}
            >
            {running && (
              <div className="absolute inset-0 z-30 flex items-center justify-center bg-white/70 backdrop-blur-[1px]" role="status" aria-label="Refreshing pivot table">
                <div className="inline-flex items-center gap-2 rounded-md border border-slate-200 bg-white px-3 py-2 text-xs font-medium text-slate-700 shadow-sm">
                  <Loader2 className="h-4 w-4 animate-spin text-blue-600" aria-hidden />
                  Updating pivot...
                </div>
              </div>
            )}
            <div
              ref={pivotHScrollRef}
              className="flex min-h-0 w-full min-w-0 flex-1 flex-col overflow-x-auto overflow-y-hidden scroll-smooth"
            >
              <div
                className="flex min-h-0 min-w-0 flex-1 flex-col"
                style={{ width: pivotOuterWidth, minWidth: pivotTableMinWidth }}
              >
                <div className="w-full shrink-0 border-b border-slate-200">
                  <table
                    className="border-separate border-spacing-0 text-[10px] table-fixed"
                    style={{ width: pivotOuterWidth, minWidth: pivotTableMinWidth }}
                  >
                    <colgroup>
                      {layoutColumnWidths.map((w, i) => (
                        <col key={`chg-${i}`} style={{ width: w, minWidth: columnWidths[i] }} />
                      ))}
                    </colgroup>
                    <thead className="bg-[#d9e1f2] text-[8px] leading-tight text-slate-900">
                      <tr>
                        {rowHeaderFields.map((rf, idx) => (
                          <th
                            key={`row-header-${rf}-${idx}`}
                            rowSpan={pivotLayout.headerDepth}
                            className={[
                              'min-w-0 border border-[#b4c6e7] bg-[#d9e1f2] px-1 py-[1px] text-left align-top font-semibold leading-snug [overflow-wrap:anywhere] break-words whitespace-normal',
                              isSelectedCell(0, idx) ? '!bg-[#b4c6e7]' : '',
                            ].join(' ')}
                            onClick={() => onCellClick(0, idx)}
                          >
                            {fieldLabel(rf)}
                          </th>
                        ))}
                        {pivotLayout.labelRows[0]?.map((g, idx) => (
                          <th
                            key={`h0-${idx}-${g.label}`}
                            colSpan={g.span}
                            className="min-w-0 border border-[#b4c6e7] bg-[#d9e1f2] px-1 py-[1px] text-left align-top font-semibold leading-snug [overflow-wrap:anywhere] break-words whitespace-normal"
                          >
                            {String(g.label).toUpperCase()}
                          </th>
                        ))}
                      </tr>
                      {pivotLayout.labelRows.slice(1).map((rowGroups, levelIdx) => (
                        <tr key={`h-level-${levelIdx + 1}`}>
                          {rowGroups.map((g, idx) => (
                            <th
                              key={`h-${levelIdx + 1}-${idx}-${g.label}`}
                              colSpan={g.span}
                              className="min-w-0 border border-[#b4c6e7] bg-[#d9e1f2] px-1 py-[1px] text-left align-top font-semibold leading-snug [overflow-wrap:anywhere] break-words whitespace-normal"
                            >
                              {String(g.label).toUpperCase()}
                            </th>
                          ))}
                        </tr>
                      ))}
                      <tr>
                        {pivotLayout.metricRow.map((m, idx) => (
                          <th
                            key={`metric-${idx}-${m}`}
                            className={[
                              'min-w-0 border border-[#8faadc] bg-[#c5d9f1] px-1 py-[1px] text-right align-top font-semibold leading-snug [overflow-wrap:anywhere] break-words whitespace-normal',
                              idx === pivotLayout.metricRow.length - 1 ? 'pr-4 sm:pr-5' : '',
                              isSelectedCell(0, rowHeaderFields.length + idx) ? '!bg-[#9bb7e8]' : '',
                            ].join(' ')}
                            onClick={() => onCellClick(0, rowHeaderFields.length + idx)}
                          >
                            {m}
                          </th>
                        ))}
                      </tr>
                    </thead>
                  </table>
                </div>
                <div
                  ref={pivotListWrapRef}
                  className="flex min-h-0 w-full min-w-0 flex-1 flex-col overflow-hidden"
                >
                  {orderedBodyRows.length > 0 ? (
                    <div
                      ref={pivotScrollRef}
                      className="pivot-body-list min-h-0 w-full scroll-smooth"
                      style={{
                        height: listViewportHeight,
                        width: '100%',
                        minHeight: 0,
                        overflowX: 'hidden',
                        overflowY: 'auto',
                        scrollBehavior: 'smooth',
                      }}
                      onScroll={onPivotVirtualScroll}
                    >
                      <div
                        style={{
                          height: rowVirtualizer.getTotalSize(),
                          width: '100%',
                          position: 'relative',
                        }}
                      >
                        {rowVirtualizer.getVirtualItems().map((vRow) => (
                          <PivotVirtualRow
                            key={vRow.key}
                            index={vRow.index}
                            style={{
                              position: 'absolute',
                              top: 0,
                              left: 0,
                              width: '100%',
                              height: `${vRow.size}px`,
                              transform: `translateY(${vRow.start}px)`,
                              boxSizing: 'border-box',
                            }}
                            {...pivotListRowProps}
                          />
                        ))}
                      </div>
                    </div>
                  ) : null}
                  {bodyLoadingMore && (
                    <div className="flex shrink-0 items-center justify-center gap-2 border-t border-slate-200 bg-slate-50 py-2 text-[11px] text-slate-600" role="status" aria-label="Loading more pivot rows">
                      <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin text-blue-600" aria-hidden />
                      Loading more rows…
                    </div>
                  )}
                </div>
                {/* Last row of the pivot: grand totals for the full filtered dataset (same as Excel). */}
                <div className="w-full shrink-0 border-t-2 border-slate-300">
                  <table
                    className="border-separate border-spacing-0 text-[9px] table-fixed"
                    style={{ width: pivotOuterWidth, minWidth: pivotTableMinWidth }}
                  >
                    <colgroup>
                      {layoutColumnWidths.map((w, i) => (
                        <col key={`cfg-${i}`} style={{ width: w, minWidth: columnWidths[i] }} />
                      ))}
                    </colgroup>
                    <tfoot className="text-[9px]" aria-label="Grand totals">
                      <tr>
                        {rowHeaderFields.map((_, idx) => (
                          <td
                            key={`grand-row-${idx}`}
                            className={[
                              'z-20 min-w-0 border border-slate-200 bg-slate-100 p-1.5 align-top font-semibold leading-snug text-slate-800 shadow-[0_-6px_12px_-4px_rgba(15,23,42,0.12)] [overflow-wrap:anywhere] break-words whitespace-normal',
                              isSelectedCell(grandFooterRowIndex, idx) ? '!bg-blue-100' : '',
                            ].join(' ')}
                            onClick={() => onCellClick(grandFooterRowIndex, idx)}
                          >
                            {idx === 0 ? 'GRAND TOTAL' : ''}
                          </td>
                        ))}
                        {pivotLayout.descriptors.map((d, dIdx) => (
                          <td
                            key={`gt-${d.kind}-${d.colKey}-${d.metricKey}`}
                            className={[
                              'z-20 min-w-0 border border-slate-200 bg-slate-200/90 p-1.5 align-top text-right text-[10px] font-bold tabular-nums leading-snug shadow-[0_-6px_12px_-4px_rgba(15,23,42,0.12)] [overflow-wrap:anywhere] break-words whitespace-normal',
                              dIdx === pivotLayout.descriptors.length - 1 ? 'pr-4 sm:pr-5' : '',
                              isSelectedCell(grandFooterRowIndex, rowHeaderFields.length + dIdx) ? '!bg-blue-100' : '',
                            ].join(' ')}
                            onClick={() => onCellClick(grandFooterRowIndex, rowHeaderFields.length + dIdx)}
                          >
                            {formatPivotCellValue(
                              d.kind === 'total'
                                ? result.grandTotals?.[d.metricKey]
                                : result.columnTotals?.[d.colKey]?.[d.metricKey],
                              formatPivotIndian,
                              d.metricKey,
                            )}
                          </td>
                        ))}
                      </tr>
                    </tfoot>
                  </table>
                </div>
              </div>
            </div>
            </div>
          </div>
        )}
        </div>
      </div>
    </section>
    <aside
      className={[
        'lg:fixed lg:right-0 lg:top-0 lg:h-screen rounded-xl lg:rounded-none border border-[#e1dfdd] lg:border-l lg:border-r-0 lg:border-y-0 bg-[#f2f2f2] text-[#323130] transition-[width] duration-200 ease-out shadow-[inset_1px_0_0_#fff] z-30',
        toolsCollapsed ? 'lg:w-[3.5rem]' : 'lg:w-[300px]',
        toolsCollapsed ? 'p-1.5 overflow-hidden' : 'p-2 lg:overflow-auto',
      ].join(' ')}
    >
      <div className={['mb-2 flex items-center', toolsCollapsed ? 'justify-center' : 'justify-between'].join(' ')}>
        {!toolsCollapsed && (
          <div>
            <h3 className="text-[13px] font-semibold leading-tight text-[#323130]">PivotTable Fields</h3>
          </div>
        )}
        <button
          type="button"
          className="inline-flex h-7 w-7 shrink-0 items-center justify-center rounded border border-[#c8c8c8] bg-white text-[#323130] hover:bg-[#f3f2f1]"
          onClick={() => setToolsCollapsed((v) => !v)}
          aria-label={toolsCollapsed ? 'Expand pivot tools' : 'Collapse pivot tools'}
          title={toolsCollapsed ? 'Expand tools' : 'Collapse tools'}
        >
          {toolsCollapsed ? <PanelRightOpen className="h-4 w-4" aria-hidden /> : <PanelRightClose className="h-4 w-4" aria-hidden />}
        </button>
      </div>

      {toolsCollapsed ? (
        <div className="mt-3 flex flex-col items-center gap-2 text-[10px] font-medium uppercase tracking-wide text-[#605e5c]">
          <span className="[writing-mode:vertical-rl] rotate-180">Pivot Tools</span>
        </div>
      ) : (
        <div className="flex h-[calc(100vh-4rem)] flex-col">
          <section
            data-pivot-drop-zone="pool"
            className={[
              'flex min-h-0 max-h-[min(280px,32vh)] flex-col overflow-hidden rounded-sm border border-[#c8c8c8] bg-white p-2 shadow-[inset_0_1px_0_#fff] transition-colors',
              dropTarget === 'pool' ? 'border-[#0078d4] bg-[#deecf9] ring-1 ring-[#0078d4]/30' : '',
            ].join(' ')}
            onDragEnter={handlePivotZoneDragEnter}
            onDragOver={(e) => handleDragOverZone(e, 'pool')}
            onDragLeave={onPivotZoneDragLeave}
            onDrop={handleDropToPool}
          >
            <div className="mb-1.5">
              <p className="text-[11px] font-semibold text-[#323130]">Choose fields to add to report</p>
            </div>
            <div className="relative">
              <Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-[#8a8886]" aria-hidden />
              <input
                value={fieldSearch}
                onChange={(e) => setFieldSearch(e.target.value)}
                placeholder="Search"
                className="h-7 w-full rounded-sm border border-[#c8c8c8] bg-white pl-7 pr-2 text-[11px] text-[#323130] placeholder:text-[#8a8886] focus:border-[#0078d4] focus:outline-none focus:ring-1 focus:ring-[#0078d4]/35"
              />
            </div>
            <div className="mt-1.5 flex-1 min-h-0 divide-y divide-[#edebe9] overflow-auto border border-[#edebe9] bg-white">
                {filteredFieldPool.map((f) => (
                  <div
                    key={`pool-${f.field}`}
                    className={[
                      'flex w-full items-stretch border-b border-[#edebe9] bg-white last:border-b-0',
                      isPivotFieldActive(f.field) ? 'bg-[#f3f2f1]' : '',
                    ].join(' ')}
                  >
                    <label
                      className="flex cursor-pointer items-center px-1.5 py-1"
                      onMouseDown={(e) => e.stopPropagation()}
                    >
                      <input
                        type="checkbox"
                        className="h-3.5 w-3.5 shrink-0 rounded border-[#8a8886] text-[#0078d4] focus:ring-[#0078d4]"
                        checked={isPivotFieldActive(f.field)}
                        onChange={(e) => {
                          e.stopPropagation();
                          togglePivotFieldActive(f.field, e.target.checked);
                        }}
                        aria-label={`${isPivotFieldActive(f.field) ? 'Remove' : 'Add'} ${fieldLabel(f.field)} from report`}
                      />
                    </label>
                    <button
                      type="button"
                      draggable
                      onDragStart={(e) => handleDragStart(e, { source: 'pool', field: f.field })}
                      onDragEnd={handleDragEnd}
                      className="flex min-w-0 flex-1 items-center gap-1 border-l border-[#edebe9] px-1.5 py-1.5 text-left text-[11px] font-normal text-[#323130] hover:bg-[#f3f2f1]"
                      title="Drag to Filters, Columns, Rows, or Σ Values; drop on list above to remove"
                    >
                      <GripVertical className="h-3.5 w-3.5 shrink-0 text-[#8a8886]" aria-hidden />
                      <span className="line-clamp-2 min-w-0 flex-1 text-left leading-snug">{fieldLabel(f.field)}</span>
                    </button>
                  </div>
                ))}
                {filteredFieldPool.length === 0 && <p className="p-2 text-[11px] text-[#605e5c]">No fields found</p>}
            </div>
          </section>
          <div className="mt-2 min-h-0 flex-1 overflow-y-auto pr-1">
            <div className="flex flex-col gap-2">
            <section className="space-y-2 rounded-sm border border-[#c8c8c8] bg-[#faf9f8] p-2 shadow-sm">
              <div
                data-pivot-drop-zone="filters"
                className={`rounded-sm border p-2 transition-colors ${dropTarget === 'filters' ? 'border-[#0078d4] bg-[#deecf9]' : 'border-[#c8c8c8] bg-white'}`}
                onDragEnter={handlePivotZoneDragEnter}
                onDragOver={(e) => handleDragOverZone(e, 'filters')}
                onDragLeave={onPivotZoneDragLeave}
                onDrop={(e) => handleDropToZone(e, 'filters')}
              >
                <div className="mb-1.5 flex items-center justify-between">
                  <div className="flex items-center gap-1.5">
                    <Funnel className="h-3.5 w-3.5 text-[#605e5c]" aria-hidden />
                    <h3 className="text-[11px] font-semibold text-[#323130]">Filters</h3>
                  </div>
                  <button type="button" onClick={() => setFilters([])} className="text-[10px] font-medium text-[#605e5c] hover:text-[#323130]">Clear</button>
                </div>
                <div className="min-h-32 space-y-2 py-0.5">
                  {filters.length === 0 && <p className="text-[11px] text-[#8a8886]">Drop fields here</p>}
                  {filters.map((f, idx) => (
                    <div
                      key={`${f.field}-${idx}`}
                      draggable
                      onDragStart={(e) => handleDragStart(e, { source: 'filters', field: f.field, filterIndex: idx })}
                      onDragEnd={handleDragEnd}
                      onDragOver={(e) => {
                        e.preventDefault();
                        e.dataTransfer.dropEffect = 'move';
                      }}
                      onDrop={(e) => handleFilterCardDrop(e, idx)}
                      className="space-y-2.5 rounded-sm border border-[#edebe9] bg-[#faf9f8] px-2.5 py-2 text-[11px] shadow-sm"
                    >
                      <div className="flex items-start gap-2">
                        <GripVertical className="mt-0.5 h-4 w-4 shrink-0 text-slate-400" aria-hidden />
                        <span className="min-w-0 flex-1 break-words text-sm font-semibold leading-snug text-slate-800 [overflow-wrap:anywhere]">
                          {fieldLabel(f.field)}
                        </span>
                        <button
                          type="button"
                          className="shrink-0 rounded p-0.5 text-slate-500 hover:bg-slate-100 hover:text-slate-800"
                          onClick={() => setFilters((arr) => arr.filter((_, i) => i !== idx))}
                          aria-label="Remove filter"
                        >
                          <X className="h-4 w-4" aria-hidden />
                        </button>
                      </div>
                      <div className="rounded-md border border-slate-200 bg-slate-50/90 p-2">
                        <div className="mb-2 flex flex-wrap items-center justify-between gap-x-3 gap-y-1.5">
                          <span className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Filter values</span>
                          <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
                            <button
                              type="button"
                              className="text-[11px] font-medium text-slate-600 underline-offset-2 hover:text-slate-900 hover:underline"
                              onClick={() => {
                                const all = (filterOptions[f.field] || []).slice();
                                setFilters((arr) => arr.map((x, i) => (i === idx ? { ...x, operator: 'in', values: all, value: all[0] || '' } : x)));
                              }}
                            >
                              Select all
                            </button>
                            <button
                              type="button"
                              className="text-[11px] font-medium text-slate-600 underline-offset-2 hover:text-slate-900 hover:underline"
                              onClick={() => {
                                setFilters((arr) => arr.map((x, i) => (i === idx ? { ...x, operator: 'in', values: [], value: '' } : x)));
                              }}
                            >
                              Clear selection
                            </button>
                            <button
                              type="button"
                              className="text-[11px] font-semibold text-blue-700 hover:text-blue-900"
                              onClick={() => loadFilterValues(f.field, { force: true })}
                            >
                              Reload values
                            </button>
                          </div>
                        </div>
                        {filterLoading[f.field] ? (
                          <div className="flex items-center gap-2 py-3 text-[11px] text-slate-600" role="status">
                            <Loader2 className="h-4 w-4 shrink-0 animate-spin text-blue-600" aria-hidden />
                            Loading…
                          </div>
                        ) : null}
                        {!filterLoading[f.field] && filterErrorByField[f.field] ? (
                          <p className="py-1 text-[11px] leading-snug text-red-600">{filterErrorByField[f.field]}</p>
                        ) : null}
                        {!filterLoading[f.field] && filterReady[f.field] && !filterErrorByField[f.field] && (filterOptions[f.field] || []).length === 0 ? (
                          <p className="py-1 text-[11px] leading-snug text-slate-500">
                            No distinct values (column empty or only blanks). Try Reload values after import.
                          </p>
                        ) : null}
                        <div className="max-h-36 overflow-y-auto overflow-x-hidden space-y-1.5 pr-0.5">
                          {(filterOptions[f.field] || []).map((opt) => {
                            const selectedVals = Array.isArray(f.values) ? f.values : [];
                            const checked = selectedVals.includes(opt);
                            return (
                              <label key={`${f.field}-chk-${opt}`} className="flex cursor-pointer items-start gap-2 text-[11px] text-slate-800">
                                <input
                                  type="checkbox"
                                  className="mt-0.5 h-3.5 w-3.5 shrink-0 rounded border-slate-400"
                                  checked={checked}
                                  onChange={() => toggleFilterMultiValue(idx, opt)}
                                />
                                <span className="min-w-0 flex-1 break-words leading-snug [overflow-wrap:anywhere]">{String(opt ?? '').toUpperCase()}</span>
                              </label>
                            );
                          })}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              <div
                data-pivot-drop-zone="columns"
                className={`rounded-sm border p-2 transition-colors ${dropTarget === 'columns' ? 'border-[#0078d4] bg-[#deecf9]' : 'border-[#c8c8c8] bg-white'}`}
                onDragEnter={handlePivotZoneDragEnter}
                onDragOver={(e) => handleDragOverZone(e, 'columns')}
                onDragLeave={onPivotZoneDragLeave}
                onDrop={(e) => handleDropToZone(e, 'columns')}
              >
                <div className="mb-1.5 flex items-center gap-1.5"><Columns3 className="h-3.5 w-3.5 text-[#605e5c]" aria-hidden /><h3 className="text-[11px] font-semibold text-[#323130]">Columns</h3></div>
                <div className="min-h-20 space-y-1.5 py-0.5">
                  {columns.length === 0 && <p className="text-[11px] text-[#8a8886]">Drop fields here</p>}
                  {columns.map((r, idx) => (
                    <div
                      key={r}
                      draggable
                      onDragStart={(e) => handleDragStart(e, { source: 'columns', field: r })}
                      onDragEnd={handleDragEnd}
                      onDragOver={(e) => {
                        e.preventDefault();
                        e.dataTransfer.dropEffect = 'move';
                      }}
                      onDrop={(e) => handleColumnDrop(e, idx)}
                      className="flex flex-wrap items-center gap-1.5 rounded-sm border border-[#edebe9] bg-[#f3f2f1] px-2 py-1.5 text-[11px]"
                    >
                      <GripVertical className="h-3 w-3 text-[#8a8886]" aria-hidden />
                      <span className="min-w-0 flex-1 break-words leading-snug text-[#323130] [overflow-wrap:anywhere] whitespace-normal">{fieldLabel(r)}</span>
                      <button type="button" className="text-[#605e5c] hover:text-[#323130]" onClick={() => setColumns((x) => x.filter((v) => v !== r))}><X className="h-3.5 w-3.5" aria-hidden /></button>
                    </div>
                  ))}
                </div>
              </div>

              <div
                data-pivot-drop-zone="rows"
                className={`rounded-sm border p-2 transition-colors ${dropTarget === 'rows' ? 'border-[#0078d4] bg-[#deecf9]' : 'border-[#c8c8c8] bg-white'}`}
                onDragEnter={handlePivotZoneDragEnter}
                onDragOver={(e) => handleDragOverZone(e, 'rows')}
                onDragLeave={onPivotZoneDragLeave}
                onDrop={(e) => handleDropToZone(e, 'rows')}
              >
                <div className="mb-1.5 flex items-center gap-1.5"><Rows3 className="h-3.5 w-3.5 text-[#605e5c]" aria-hidden /><h3 className="text-[11px] font-semibold text-[#323130]">Rows</h3></div>
                <div className="relative mb-1.5">
                  <Search className="pointer-events-none absolute left-2 top-1/2 h-3 w-3 -translate-y-1/2 text-[#8a8886]" aria-hidden />
                  <input
                    value={rowSearch}
                    onChange={(e) => setRowSearch(e.target.value)}
                    placeholder="Search rows"
                    className="h-7 w-full rounded-sm border border-[#c8c8c8] bg-white pl-6 pr-2 text-[10px] text-[#323130] placeholder:text-[#8a8886] focus:border-[#0078d4] focus:outline-none focus:ring-1 focus:ring-[#0078d4]/35"
                  />
                </div>
                <div className="min-h-20 max-h-40 space-y-1.5 overflow-auto py-0.5 pr-0.5">
                  {rows.length === 0 && <p className="text-[11px] text-[#8a8886]">Drop fields here</p>}
                  {rows.length > 0 && filteredRowItems.length === 0 && <p className="text-[11px] text-[#8a8886]">No rows matched</p>}
                  {filteredRowItems.map((r) => {
                    const rowIdx = rows.indexOf(r);
                    return (
                    <div
                      key={r}
                      draggable
                      onDragStart={(e) => handleDragStart(e, { source: 'rows', field: r })}
                      onDragEnd={handleDragEnd}
                      onDragOver={(e) => {
                        e.preventDefault();
                        e.dataTransfer.dropEffect = 'move';
                      }}
                      onDrop={(e) => handleRowDrop(e, rowIdx)}
                      className="flex flex-wrap items-center gap-1.5 rounded-sm border border-[#edebe9] bg-[#f3f2f1] px-2 py-1.5 text-[11px]"
                    >
                      <span className="min-w-0 flex-1 break-words leading-snug text-[#323130] [overflow-wrap:anywhere] whitespace-normal">{fieldLabel(r)}</span>
                      <label
                        className="inline-flex shrink-0 cursor-pointer items-center gap-1 rounded-sm border border-[#c8c8c8] bg-white px-1.5 py-0.5 text-[9px] text-[#605e5c]"
                        onMouseDown={(e) => e.stopPropagation()}
                      >
                        <input
                          type="checkbox"
                          className="h-3 w-3 rounded border-[#8a8886] bg-white"
                          checked={subtotalFields.includes(r)}
                          onMouseDown={(e) => e.stopPropagation()}
                          onChange={(e) => {
                            e.stopPropagation();
                            const checked = e.target.checked;
                            setSubtotalFields((prev) => (checked ? [...new Set([...prev, r])] : prev.filter((x) => x !== r)));
                          }}
                        />
                        Subtotal
                      </label>
                      <GripVertical className="h-3 w-3 text-[#8a8886]" aria-hidden />
                      <button type="button" className="text-[#605e5c] hover:text-[#323130]" onClick={() => setRows((x) => x.filter((v) => v !== r))}><X className="h-3.5 w-3.5" aria-hidden /></button>
                    </div>
                    );
                  })}
                </div>
              </div>

              <div
                data-pivot-drop-zone="values"
                className={`rounded-sm border p-2 transition-colors ${dropTarget === 'values' ? 'border-[#0078d4] bg-[#deecf9]' : 'border-[#c8c8c8] bg-white'}`}
                onDragEnter={handlePivotZoneDragEnter}
                onDragOver={(e) => handleDragOverZone(e, 'values')}
                onDragLeave={onPivotZoneDragLeave}
                onDrop={(e) => handleDropToZone(e, 'values')}
              >
                <div className="mb-1.5 flex items-center gap-1.5"><Sigma className="h-3.5 w-3.5 text-[#605e5c]" aria-hidden /><h3 className="text-[11px] font-semibold text-[#323130]">Σ Values</h3></div>
                <div className="min-h-20 space-y-1.5 py-0.5">
                  {values.length === 0 && (
                    <p className="text-[11px] text-[#8a8886]">
                      {rows.length > 0 || columns.length > 0
                        ? 'Drop measures here (default: Count of rows).'
                        : 'Drop fields here'}
                    </p>
                  )}
                  {values.map((v, idx) => (
                    <div
                      key={`${v.field}-${idx}`}
                      draggable
                      onDragStart={(e) => handleDragStart(e, { source: 'values', field: v.field, agg: v.agg, valueIndex: idx })}
                      onDragEnd={handleDragEnd}
                      onDragOver={(e) => {
                        e.preventDefault();
                        e.dataTransfer.dropEffect = 'move';
                      }}
                      onDrop={(e) => handleValueDrop(e, idx)}
                      className="flex items-center gap-1.5 rounded-sm border border-[#edebe9] bg-[#f3f2f1] px-2 py-1.5 text-[11px]"
                    >
                      <GripVertical className="h-3 w-3 text-[#8a8886]" aria-hidden />
                      <span className="min-w-0 flex-1 break-words leading-snug text-[#323130] [overflow-wrap:anywhere] whitespace-normal">{fieldLabel(v.field)}</span>
                      <select className="h-6 rounded-sm border border-[#c8c8c8] bg-white px-1.5 text-[10px] text-[#323130] focus:border-[#0078d4] focus:outline-none focus:ring-1 focus:ring-[#0078d4]/35" value={v.agg} onChange={(e) => setValues((arr) => arr.map((x, i) => (i === idx ? { ...x, agg: e.target.value, label: `${e.target.value.toUpperCase()} ${x.field}` } : x)))}>
                        {AGG_OPTIONS.map((a) => <option key={a} value={a}>{a.toUpperCase()}</option>)}
                      </select>
                      <button type="button" className="text-[#605e5c] hover:text-[#323130]" onClick={() => setValues((arr) => arr.filter((_, i) => i !== idx))}><X className="h-3.5 w-3.5" aria-hidden /></button>
                    </div>
                  ))}
                </div>
              </div>
            </section>
          </div>

          </div>
        </div>
      )}
    </aside>
    </div>
  );
}

