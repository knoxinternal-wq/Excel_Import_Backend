import { useState, useEffect, useRef } from 'react';
import { Trash2, Loader2, CalendarRange, AlertTriangle } from 'lucide-react';
import { dataApi } from '../services/api';
import { LONG_RUNNING_REQUEST_MS, MAX_UI_DATA_LOAD_MS } from '../constants/timing';
import { formatRequestError } from '../utils/requestError';

/**
 * Bulk delete sales_data rows by bill_date range (server-side single DELETE).
 * @param {{ onDeleted?: () => void, onNotify?: (toast: { type: string, message: string }) => void }} props
 */
export default function DeleteByDate({ onDeleted, onNotify }) {
  const [fromDate, setFromDate] = useState('');
  const [toDate, setToDate] = useState('');
  const [previewCount, setPreviewCount] = useState(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const abortRef = useRef(null);

  useEffect(() => {
    setPreviewCount(null);
    if (!fromDate || !toDate) {
      return undefined;
    }
    if (fromDate > toDate) {
      return undefined;
    }

    abortRef.current?.abort();
    const ctrl = new AbortController();
    abortRef.current = ctrl;

    const timer = setTimeout(async () => {
      setPreviewLoading(true);
      try {
        const { data } = await dataApi.previewDeleteRange(
          { fromDate, toDate },
          { signal: ctrl.signal },
        );
        if (!ctrl.signal.aborted && data?.success) {
          setPreviewCount(Number(data.count) || 0);
        }
      } catch (e) {
        if (e?.name === 'CanceledError' || e?.code === 'ERR_CANCELED') return;
        if (!ctrl.signal.aborted) {
          setPreviewCount(null);
          onNotify?.({
            type: 'error',
            message: formatRequestError(e, 'Could not load preview count.', { timeoutMs: MAX_UI_DATA_LOAD_MS }),
          });
        }
      } finally {
        if (!ctrl.signal.aborted) setPreviewLoading(false);
      }
    }, 400);

    return () => {
      clearTimeout(timer);
      ctrl.abort();
    };
  }, [fromDate, toDate, onNotify]);

  const rangeInvalid = Boolean(fromDate && toDate && fromDate > toDate);
  const canDelete =
    Boolean(fromDate && toDate && !rangeInvalid && !deleting && !previewLoading && previewCount !== null);

  const handleDelete = async () => {
    if (!canDelete) return;
    const previewLabel = previewCount === 1 ? '1 row' : `${previewCount?.toLocaleString() || 0} rows`;
    if (!window.confirm(`Delete ${previewLabel} from ${fromDate} to ${toDate}? This action cannot be undone.`)) return;

    setDeleting(true);
    try {
      const { data } = await dataApi.deleteByDateRange({ fromDate, toDate });
      if (data?.success) {
        const n = Number(data.deletedRows) || 0;
        onNotify?.({
          type: 'success',
          message: `Deleted ${n.toLocaleString()} rows successfully.`,
        });
        setPreviewCount(0);
        onDeleted?.();
      } else {
        onNotify?.({ type: 'error', message: data?.error || 'Delete failed.' });
      }
    } catch (e) {
      onNotify?.({
        type: 'error',
        message: formatRequestError(e, 'Delete failed.', { timeoutMs: LONG_RUNNING_REQUEST_MS }),
      });
    } finally {
      setDeleting(false);
    }
  };

  return (
    <section
      className="w-full overflow-hidden rounded-lg border border-slate-200 bg-white shadow-sm"
      aria-labelledby="delete-range-heading"
    >
      <div className="border-b border-slate-200 bg-gradient-to-r from-slate-50 to-slate-100/80 px-4 py-3 sm:px-5">
        <div className="flex items-start gap-3">
          <div
            className="mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-lg border border-slate-200/80 bg-white text-slate-500 shadow-sm"
            aria-hidden
          >
            <CalendarRange className="h-4 w-4" />
          </div>
          <div className="min-w-0 flex-1">
            <h3 id="delete-range-heading" className="text-sm font-semibold text-slate-800">
              Delete data by date range
            </h3>
            <p className="mt-0.5 text-xs leading-relaxed text-slate-600">
              Removes rows whose <span className="font-medium text-slate-700">BILL Date</span> is between the
              dates below (inclusive). Uses the indexed <code className="rounded bg-white/80 px-1 py-0.5 text-[11px] text-slate-700 ring-1 ring-slate-200/80">bill_date</code> column — no row-by-row delete.
            </p>
          </div>
        </div>
      </div>

      <div className="px-4 py-4 sm:px-5 sm:py-5">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:gap-6">
          <div className="grid flex-1 grid-cols-1 gap-3 sm:grid-cols-2 sm:gap-4">
            <label className="block text-xs font-medium text-slate-600">
              From date
              <input
                type="date"
                value={fromDate}
                onChange={(e) => setFromDate(e.target.value)}
                className="mt-1.5 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm transition-colors focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500/20"
              />
            </label>
            <label className="block text-xs font-medium text-slate-600">
              To date
              <input
                type="date"
                value={toDate}
                onChange={(e) => setToDate(e.target.value)}
                className="mt-1.5 w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm transition-colors focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500/20"
              />
            </label>
          </div>

          <div className="flex flex-col gap-3 border-t border-slate-100 pt-4 lg:w-[min(100%,20rem)] lg:shrink-0 lg:border-l lg:border-t-0 lg:pl-6 lg:pt-0">
            <div className="min-h-[2.75rem] text-sm" aria-live="polite">
              {rangeInvalid && (
                <span className="inline-flex items-start gap-2 rounded-md bg-amber-50 px-2.5 py-2 text-amber-900 ring-1 ring-amber-200/80">
                  <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" aria-hidden />
                  <span>From date must be on or before To date.</span>
                </span>
              )}
              {!rangeInvalid && previewLoading && (
                <span className="inline-flex items-center gap-2 text-slate-500">
                  <Loader2 className="h-4 w-4 animate-spin text-indigo-600" aria-hidden />
                  Counting matching rows…
                </span>
              )}
              {!rangeInvalid && !previewLoading && previewCount !== null && fromDate && toDate && (
                <span className="inline-flex items-start gap-2 rounded-md bg-amber-50/90 px-2.5 py-2 text-slate-800 ring-1 ring-amber-200/70">
                  <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" aria-hidden />
                  <span>
                    <strong className="font-semibold tabular-nums text-slate-900">
                      {previewCount.toLocaleString()}
                    </strong>{' '}
                    {previewCount === 1 ? 'row will be' : 'rows will be'} deleted
                  </span>
                </span>
              )}
            </div>

            <button
              type="button"
              onClick={handleDelete}
              disabled={!canDelete}
              className="inline-flex w-full items-center justify-center gap-2 rounded-lg border border-rose-200 bg-rose-600 px-4 py-2.5 text-sm font-semibold text-white shadow-sm transition-colors hover:bg-rose-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-rose-500 focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:border-slate-200 disabled:bg-slate-200 disabled:text-slate-500 disabled:shadow-none"
            >
              {deleting ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
                  Deleting…
                </>
              ) : (
                <>
                  <Trash2 className="h-4 w-4" aria-hidden />
                  Delete in range
                </>
              )}
            </button>
          </div>
        </div>
      </div>
    </section>
  );
}
