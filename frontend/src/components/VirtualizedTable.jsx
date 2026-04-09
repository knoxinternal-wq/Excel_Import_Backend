import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { isCancel as axiosIsCancel } from 'axios';
import { Table2, ChevronLeft, ChevronRight, ChevronDown, Loader2, Columns3 } from 'lucide-react';
import { dataApi } from '../services/api';
import usePageVisibility from '../hooks/usePageVisibility';
import { formatRequestError } from '../utils/requestError';
import VirtualizedTableRow from './VirtualizedTableRow';

const ROW_HEIGHT = 40;
const HEADER_HEIGHT = 36;
/** Max rows worth of viewport height before relying on inner scroll (avoids hiding most of a page). */
const VIEWPORT_ROW_CAP = 120;
const DEFAULT_PAGE_SIZE = 100;
/** Backend allows 10–300; keep options aligned with UX reference. */
const PAGE_SIZE_OPTIONS = [25, 50, 100, 200];
/** Use keyset (id) from page 2+ so deep pages avoid OFFSET over millions of rows. */
const KEYSET_FROM_PAGE = 2;

const COLUMNS = [
  { key: 'branch', label: 'Branch', width: 160 },
  { key: 'fy', label: 'FY', width: 110 },
  { key: 'month', label: 'MONTH', width: 120 },
  { key: 'mmm', label: 'MMM', width: 95 },
  { key: 'region', label: 'REGION', width: 160 },
  { key: 'state', label: 'STATE', width: 160 },
  { key: 'district', label: 'DISTRICT', width: 160 },
  { key: 'city', label: 'CITY', width: 160 },
  { key: 'business_type', label: 'TYPE OF Business', width: 160 },
  { key: 'agent_names_correction', label: 'Agent Names Correction', width: 220 },
  { key: 'party_grouped', label: 'PARTY GROUPED', width: 280 },
  { key: 'party_name_for_count', label: 'PARTY NAME FOR COUNT', width: 280 },
  { key: 'brand', label: 'BRAND', width: 160 },
  { key: 'agent_name', label: 'AGENT NAME', width: 180 },
  { key: 'to_party_name', label: 'TO PARTY NAME', width: 340 },
  { key: 'bill_no', label: 'BILL NO.', width: 160 },
  { key: 'bill_date', label: 'BILL Date', width: 115 },
  { key: 'item_no', label: 'ITEM NO', width: 140 },
  { key: 'shade_name', label: 'SHADE NAME', width: 160 },
  { key: 'rate_unit', label: 'RATE/UNIT', width: 130 },
  { key: 'size', label: 'SIZE', width: 110 },
  { key: 'units_pack', label: 'UNITS/PACK', width: 140 },
  { key: 'sl_qty', label: 'SL QTY', width: 110 },
  { key: 'gross_amount', label: 'GROSS AMOUNT', width: 160 },
  { key: 'amount_before_tax', label: 'AMOUNT BEFORE TAX', width: 170 },
  { key: 'net_amount', label: 'NET AMOUNT', width: 160 },
  { key: 'sale_order_no', label: 'SALE ORDER NO.', width: 220 },
  { key: 'sale_order_date', label: 'SALE ORDER Date', width: 160 },
  { key: 'item_with_shade', label: 'Item with Shade', width: 280 },
  { key: 'item_category', label: 'Item Category', width: 180 },
  { key: 'item_sub_cat', label: 'Item Sub cat', width: 150 },
  { key: 'so_type', label: 'SO TYPE', width: 130 },
  { key: 'scheme', label: 'SCHEME', width: 130 },
  { key: 'goods_type', label: 'GOODS TYPE', width: 140 },
  { key: 'agent_name_final', label: 'AGENT NAME.', width: 180 },
  { key: 'pin_code', label: 'PIN CODE', width: 130 },
];


function isRequestAbortedError(err) {
  if (!err) return false;
  if (err.name === 'AbortError' || err.name === 'CanceledError') return true;
  if (err.code === 'ERR_CANCELED') return true;
  try {
    if (axiosIsCancel(err)) return true;
  } catch {
    /* ignore */
  }
  return false;
}

function paginationFromApi(pag, page, limitFallback) {
  const requestedPage = pag.page ?? page;
  const totalPages = Math.max(1, pag.totalPages ?? 1);
  const safePage = Math.min(Math.max(1, requestedPage), totalPages);
  return {
    page: safePage,
    limit: pag.limit ?? limitFallback,
    total: pag.total != null ? pag.total : null,
    totalPages,
    countEstimated: pag.countEstimated === true,
  };
}

/** API-shaped payload from prefetch after import (`data` + `pagination`). */
export default function VirtualizedTable({ refreshInterval, refreshTrigger, bootstrap = null }) {
  /** Capture bootstrap only on first paint so parent can clear it without re-running a cold load. */
  const [initialBootstrap] = useState(() => bootstrap);
  const hadBootstrap = initialBootstrap != null;
  const initialRows =
    hadBootstrap && Array.isArray(initialBootstrap.data) ? initialBootstrap.data : [];
  const initialLimit =
    hadBootstrap && initialBootstrap.pagination?.limit != null
      ? Math.min(300, Math.max(10, Number(initialBootstrap.pagination.limit) || DEFAULT_PAGE_SIZE))
      : DEFAULT_PAGE_SIZE;
  const initialPag = hadBootstrap
    ? paginationFromApi(initialBootstrap.pagination ?? {}, 1, initialLimit)
    : { page: 1, limit: initialLimit, total: null, totalPages: 1, countEstimated: false };

  const parentRef = useRef(null);
  const pageRef = useRef(1);
  const requestAbortRef = useRef(null);
  const pageCursorRef = useRef({ 1: null });
  const dataRef = useRef([]);
  const paginationRef = useRef({
    page: initialPag.page ?? 1,
    limit: initialPag.limit ?? initialLimit,
    total: initialPag.total ?? null,
    totalPages: initialPag.totalPages ?? 1,
    countEstimated: initialPag.countEstimated === true,
  });

  const [pageSize, setPageSize] = useState(() => initialLimit);
  const [data, setData] = useState(() => initialRows);
  const [pagination, setPagination] = useState(() => initialPag);
  const [loading, setLoading] = useState(() => !hadBootstrap);
  const [isPageLoading, setIsPageLoading] = useState(false);
  const [error, setError] = useState(null);
  const [visibleColumns, setVisibleColumns] = useState(() => COLUMNS.map((c) => c.key));
  const [showColumnsPanel, setShowColumnsPanel] = useState(false);
  const [nextPagePrefetch, setNextPagePrefetch] = useState(null);
  const pageVisible = usePageVisibility();
  const tabWasHiddenRef = useRef(false);

  const displayColumns = useMemo(() => {
    const keep = new Set(visibleColumns);
    const cols = COLUMNS.filter((c) => keep.has(c.key));
    return cols.length ? cols : [COLUMNS[0]];
  }, [visibleColumns]);
  const tableTotalWidth = useMemo(
    () => displayColumns.reduce((a, c) => a + c.width, 0),
    [displayColumns],
  );

  const effectiveRefreshMs = useMemo(() => {
    if (!refreshInterval || refreshInterval <= 0) return 0;
    if (!pageVisible) return Math.max(refreshInterval * 4, 60_000);
    return refreshInterval;
  }, [refreshInterval, pageVisible]);

  useEffect(() => {
    paginationRef.current = pagination;
  }, [pagination]);

  useEffect(() => {
    dataRef.current = data;
  }, [data]);

  const fetchData = useCallback(async (page = 1, silent = false, limitOverride = null) => {
    const limit = Math.min(300, Math.max(10, limitOverride ?? pageSize));
    // Silent polls / tab refresh: skip COUNT(*) on page 1 (expensive at ~18L rows); keep prior total below.
    const includeTotal = page === 1 && !silent ? 1 : 0;
    // Keep in sync immediately so interval/visibility refresh never targets the previous page while a navigation fetch is in flight.
    pageRef.current = page;
    let requestAborted = false;
    if (!silent) {
      if (dataRef.current.length > 0) setIsPageLoading(true);
      else setLoading(true);
    }
    try {
      if (requestAbortRef.current) requestAbortRef.current.abort();
      const controller = new AbortController();
      requestAbortRef.current = controller;

      const useKeyset = page >= KEYSET_FROM_PAGE;
      const keysetCursor = useKeyset ? (pageCursorRef.current[page] ?? null) : null;
      const { data: res } = await dataApi.fetch({
        page,
        limit,
        includeTotal,
        sortBy: 'id',
        sortOrder: 'desc',
        paging: useKeyset ? 'keyset' : 'offset',
        cursorId: keysetCursor ?? undefined,
      }, { signal: controller.signal });
      const rows = Array.isArray(res?.data) ? res.data : [];
      const pag = res?.pagination ?? {};
      if (rows.length > 0) {
        pageCursorRef.current[page + 1] = pag?.nextCursor ?? rows[rows.length - 1]?.id ?? null;
      }
      setError(null);
      const requestedPage = pag.page ?? page;
      const nextPag = paginationFromApi(pag, page, limit);
      const { page: safePage } = nextPag;
      const prevPag = paginationRef.current;
      let mergedPag = nextPag;
      // Page 2+ and silent page-1 polls use includeTotal=0 (no COUNT). Merge the last known row total
      // from the previous response — but always recompute totalPages from that total so a stale
      // totalPages (e.g. 1 after bootstrap) does not permanently disable Next.
      if (mergedPag.total == null && prevPag.total != null) {
        const total = Number(prevPag.total);
        if (Number.isFinite(total) && total >= 0) {
          const tpFromTotal = Math.max(1, Math.ceil(total / limit));
          mergedPag = {
            ...mergedPag,
            total,
            totalPages: Math.max(tpFromTotal, mergedPag.totalPages ?? 1),
            countEstimated: prevPag.countEstimated === true,
          };
        }
      }
      setPagination(mergedPag);
      paginationRef.current = mergedPag;
      pageRef.current = mergedPag.page;
      if (safePage !== requestedPage) {
        setData([]);
        await fetchData(safePage, true, limit);
        return;
      }
      setData(rows);
      const hasNextPage = mergedPag.page < mergedPag.totalPages;
      if (hasNextPage && rows.length > 0) {
        const nextPage = mergedPag.page + 1;
        dataApi.fetch({
          page: nextPage,
          limit,
          includeTotal: 0,
          sortBy: 'id',
          sortOrder: 'desc',
          paging: nextPage >= KEYSET_FROM_PAGE ? 'keyset' : 'offset',
          cursorId: nextPage >= KEYSET_FROM_PAGE ? (pageCursorRef.current[nextPage] ?? res?.pagination?.nextCursor ?? undefined) : undefined,
        })
          .then((pref) => setNextPagePrefetch({
            page: nextPage,
            rows: Array.isArray(pref?.data?.data) ? pref.data.data : [],
            pagination: pref?.data?.pagination ?? null,
          }))
          .catch(() => setNextPagePrefetch(null));
      } else {
        setNextPagePrefetch(null);
      }
    } catch (err) {
      if (isRequestAbortedError(err)) {
        requestAborted = true;
        return;
      }
      if (!silent) {
        setError(formatRequestError(err, 'Failed to load data'));
      }
    } finally {
      if (requestAborted) return;
      setIsPageLoading(false);
      if (!silent) setLoading(false);
    }
  }, [pageSize]);

  // After switching back to this browser tab, refresh the current page (avoids stale data until the next poll).
  useEffect(() => {
    if (!pageVisible) {
      tabWasHiddenRef.current = true;
      return;
    }
    if (tabWasHiddenRef.current) {
      tabWasHiddenRef.current = false;
      fetchData(pageRef.current, true);
    }
  }, [pageVisible, fetchData]);

  useEffect(() => () => {
    if (requestAbortRef.current) requestAbortRef.current.abort();
  }, []);

  useEffect(() => {
    if (hadBootstrap) {
      fetchData(1, true);
      return;
    }
    fetchData(1, false);
  }, [fetchData, hadBootstrap]);

  const goToPage = useCallback((page) => {
    const tp = Math.max(1, pagination.totalPages || 1);
    const safe = Math.min(Math.max(1, page), tp);
    pageRef.current = safe;
    if (nextPagePrefetch?.page === safe && Array.isArray(nextPagePrefetch.rows)) {
      setData(nextPagePrefetch.rows);
      if (nextPagePrefetch.pagination) {
        const np = paginationFromApi(nextPagePrefetch.pagination, safe, pageSize);
        setPagination(np);
        paginationRef.current = np;
      }
      fetchData(safe, true);
      return;
    }
    fetchData(safe, false);
  }, [fetchData, pagination.totalPages, nextPagePrefetch, pageSize]);

  const { footerRangeText, footerRangeTitle } = useMemo(() => {
    const p = pagination.page || 1;
    const lim = pagination.limit || pageSize;
    const n = data.length;
    const start = n === 0 ? 0 : ((p - 1) * lim) + 1;
    const end = n === 0 ? 0 : start + n - 1;
    const total = pagination.total;
    const approx = pagination.countEstimated === true;
    const fmt = (x) => x.toLocaleString('en-IN');

    if (loading && n === 0) {
      return { footerRangeText: '—', footerRangeTitle: 'Loading…' };
    }
    return {
      footerRangeText:
        total != null
          ? `${fmt(start)}-${fmt(end)} of ${approx ? '~' : ''}${fmt(total)}`
          : `${fmt(start)}-${fmt(end)}`,
      footerRangeTitle: `Page ${fmt(p)}: showing rows ${fmt(start)} to ${fmt(end)}${
        total != null
          ? ` out of ${approx ? 'about ' : ''}${fmt(total)}${approx ? ' (estimate from DB stats)' : ''}`
          : ''
      }.`,
    };
  }, [
    loading,
    pagination.page,
    pagination.limit,
    pagination.total,
    pagination.countEstimated,
    pageSize,
    data.length,
  ]);

  const handlePageSizeChange = useCallback((e) => {
    const next = Math.min(300, Math.max(10, Number(e.target.value) || DEFAULT_PAGE_SIZE));
    setPageSize(next);
    pageCursorRef.current = { 1: null };
    pageRef.current = 1;
    fetchData(1, false, next);
  }, [fetchData]);

  useEffect(() => {
    if (!effectiveRefreshMs) return;
    const iv = setInterval(() => fetchData(pageRef.current, true), effectiveRefreshMs);
    return () => clearInterval(iv);
  }, [effectiveRefreshMs, fetchData]);

  useEffect(() => {
    if (typeof refreshTrigger === 'number' ? refreshTrigger > 0 : !!refreshTrigger) {
      fetchData(pageRef.current, true);
    }
  }, [refreshTrigger, fetchData]);

  useEffect(() => {
    const el = parentRef.current;
    if (el) el.scrollTop = 0;
  }, [pagination.page]);

  const setParentRef = useCallback((el) => {
    parentRef.current = el;
  }, []);

  const rowVirtualizer = useVirtualizer({
    count: data.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 6,
  });

  const virtualItems = rowVirtualizer.getVirtualItems();
  const virtualRowsToRender = virtualItems.length > 0
    ? virtualItems
    : Array.from({ length: Math.min(data.length, 12) }, (_, i) => ({ key: i, index: i, start: i * ROW_HEIGHT, size: ROW_HEIGHT }));

  const pageSizeSelectOptions = useMemo(() => {
    const s = new Set(PAGE_SIZE_OPTIONS);
    s.add(pageSize);
    return [...s].sort((a, b) => a - b);
  }, [pageSize]);

  return (
    <div className="space-y-3">
      <div className="flex min-h-0 flex-col overflow-hidden rounded-lg border border-slate-200 bg-white shadow-sm">
      {loading ? (
        <div className="py-12 px-4 text-slate-500 min-h-[420px]" role="status" aria-label="Loading sales data">
          <div className="flex items-center justify-center gap-2 mb-5">
            <Loader2 className="w-5 h-5 animate-spin text-indigo-600" aria-hidden />
            <p className="text-sm font-medium text-slate-700">Loading data…</p>
          </div>
          <div className="space-y-2">
            {Array.from({ length: 10 }).map((_, idx) => (
              <div key={`sk-${idx}`} className="h-8 rounded bg-slate-100 animate-pulse" />
            ))}
          </div>
        </div>
      ) : error ? (
        <div className="py-24 flex flex-col items-center justify-center text-slate-600 min-h-[420px]">
          <p className="text-sm font-medium mb-4 max-w-sm text-center">{error}</p>
          <button
            type="button"
            onClick={() => { setError(null); fetchData(1, false); }}
            className="px-5 py-2.5 bg-indigo-600 text-white rounded-xl text-sm font-medium hover:bg-indigo-700 transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 focus-visible:ring-offset-2"
          >
            Retry
          </button>
        </div>
      ) : data.length === 0 ? (
        <div className="py-24 flex flex-col items-center justify-center text-slate-500 min-h-[420px]">
          <div className="w-14 h-14 rounded-2xl bg-slate-100 flex items-center justify-center mb-4">
            <Table2 className="w-7 h-7 text-slate-400" aria-hidden />
          </div>
          <p className="text-sm font-medium text-slate-600">No data yet</p>
          <p className="text-xs text-slate-400 mt-1">Import a sales Excel file from the Import tab to get started.</p>
        </div>
      ) : (
        <div className="relative">
          {isPageLoading && (
            <div className="absolute inset-0 z-20 flex items-center justify-center bg-white/60 backdrop-blur-[1px]" role="status" aria-label="Loading page">
              <div className="inline-flex items-center gap-2 rounded-md border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 shadow-sm">
                <Loader2 className="w-3.5 h-3.5 animate-spin text-indigo-600" aria-hidden />
                Loading page...
              </div>
            </div>
          )}
          <div
            ref={setParentRef}
            className="overflow-x-auto overflow-y-auto scroll-smooth"
            style={{
              height: Math.max(
                400,
                ROW_HEIGHT
                  * Math.min(
                    Math.max(data.length, 1),
                    pagination.limit || pageSize,
                    VIEWPORT_ROW_CAP,
                  ),
              ),
            }}
          >
            <div style={{ minWidth: tableTotalWidth, width: tableTotalWidth }}>
              <div
                className="sticky top-0 z-10 flex items-center border-b border-slate-200 bg-slate-100/95 backdrop-blur-sm"
                style={{ height: HEADER_HEIGHT, width: tableTotalWidth, minWidth: tableTotalWidth, boxSizing: 'border-box' }}
              >
                {displayColumns.map((col) => (
                  <div
                    key={col.key}
                    className="flex flex-shrink-0 items-center px-3 py-2 text-left text-[11px] font-semibold uppercase tracking-wide text-slate-500 bg-slate-100/95"
                    style={{ width: col.width, minWidth: col.width, boxSizing: 'border-box' }}
                  >
                    <span className="block truncate">{col.label}</span>
                  </div>
                ))}
              </div>

              {/* Virtualized rows */}
              <div
                style={{
                  height: `${Math.max(rowVirtualizer.getTotalSize(), ROW_HEIGHT * Math.min(data.length, 10))}px`,
                  width: tableTotalWidth,
                  position: 'relative',
                }}
              >
                {virtualRowsToRender.map((virtualRow) => (
                  <VirtualizedTableRow
                    key={virtualRow.key}
                    virtualRow={virtualRow}
                    row={data[virtualRow.index]}
                    columns={displayColumns}
                    totalWidth={tableTotalWidth}
                  />
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      <div
        className="mt-auto flex flex-col items-stretch justify-end gap-3 border-t border-slate-200 bg-white px-4 py-3 sm:flex-row sm:items-center sm:gap-6"
        role="navigation"
        aria-label="Table pagination"
      >
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setShowColumnsPanel((v) => !v)}
            className="inline-flex h-9 items-center gap-1 rounded-md border border-slate-200 bg-white px-3 text-xs font-medium text-slate-700 hover:bg-slate-50"
          >
            <Columns3 className="h-4 w-4" aria-hidden />
            Columns
          </button>
          {showColumnsPanel && (
            <div className="max-h-28 overflow-auto rounded-md border border-slate-200 bg-white p-2 text-xs text-slate-700">
              <div className="grid grid-cols-2 gap-x-4 gap-y-1">
                {COLUMNS.map((col) => {
                  const checked = visibleColumns.includes(col.key);
                  return (
                    <label key={`col-toggle-${col.key}`} className="inline-flex items-center gap-1.5">
                      <input
                        type="checkbox"
                        checked={checked}
                        onChange={(e) => {
                          const isOn = e.target.checked;
                          setVisibleColumns((prev) => {
                            if (isOn) return [...new Set([...prev, col.key])];
                            const next = prev.filter((k) => k !== col.key);
                            return next.length ? next : [col.key];
                          });
                        }}
                      />
                      <span>{col.label}</span>
                    </label>
                  );
                })}
              </div>
            </div>
          )}
        </div>
        <div className="flex flex-wrap items-center justify-end gap-6 sm:ml-auto">
          <label className="flex cursor-pointer items-center gap-2 text-sm text-slate-600">
            <span className="whitespace-nowrap">Rows per page</span>
            <div className="relative">
              <select
                value={pageSize}
                onChange={handlePageSizeChange}
                disabled={loading && data.length === 0}
                className="h-9 cursor-pointer appearance-none rounded-md border border-slate-200 bg-white py-1.5 pl-3 pr-8 text-sm font-medium text-slate-800 shadow-sm outline-none transition-colors hover:border-slate-300 focus:border-blue-500 focus:ring-1 focus:ring-blue-500 disabled:cursor-not-allowed disabled:opacity-50"
                aria-label="Rows per page"
              >
                {pageSizeSelectOptions.map((n) => (
                  <option key={n} value={n}>{n}</option>
                ))}
              </select>
              <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" aria-hidden />
            </div>
          </label>
          <span
            className="text-right text-sm font-semibold tabular-nums tracking-tight text-slate-700"
            title={footerRangeTitle}
          >
            {footerRangeText}
          </span>
          <div className="flex items-center gap-0.5">
            <button
              type="button"
              onClick={() => goToPage(pagination.page - 1)}
              disabled={pagination.page <= 1 || (loading && data.length === 0)}
              aria-label="Previous page"
              className="inline-flex h-9 w-9 items-center justify-center rounded-md text-slate-500 transition-colors hover:bg-slate-100 hover:text-slate-800 disabled:cursor-not-allowed disabled:opacity-30"
            >
              <ChevronLeft className="h-5 w-5" strokeWidth={2} aria-hidden />
            </button>
            <button
              type="button"
              onClick={() => goToPage(pagination.page + 1)}
              disabled={pagination.page >= pagination.totalPages || (loading && data.length === 0)}
              aria-label="Next page"
              className="inline-flex h-9 w-9 items-center justify-center rounded-md text-slate-500 transition-colors hover:bg-slate-100 hover:text-slate-800 disabled:cursor-not-allowed disabled:opacity-30"
            >
              <ChevronRight className="h-5 w-5" strokeWidth={2} aria-hidden />
            </button>
          </div>
        </div>
      </div>
      </div>
    </div>
  );
}
