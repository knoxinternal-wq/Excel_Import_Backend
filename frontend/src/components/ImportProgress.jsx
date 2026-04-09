import { useEffect, useRef, useState } from 'react';
import { importApi } from '../services/api';
import usePageVisibility from '../hooks/usePageVisibility';

export default function ImportProgress({ jobId, onComplete, onStatusChange }) {
  const [status, setStatus] = useState(null);
  const [error, setError] = useState(null);
  const visible = usePageVisibility();
  const onCompleteRef = useRef(onComplete);
  const onStatusChangeRef = useRef(onStatusChange);
  onCompleteRef.current = onComplete;
  onStatusChangeRef.current = onStatusChange;

  useEffect(() => {
    if (!jobId) return undefined;

    const ac = new AbortController();
    let timeoutId;
    let attempts = 0;
    let consecutiveErrors = 0;

    const poll = async () => {
      try {
        const { data } = await importApi.getStatus(jobId, { signal: ac.signal });
        consecutiveErrors = 0;
        setStatus(data);
        onStatusChangeRef.current?.(data);

        if (data.status === 'completed' || data.status === 'failed' || data.status === 'cancelled') {
          onCompleteRef.current?.(data);
          return;
        }

        attempts += 1;
        const baseMs = visible ? 450 : 2800;
        const backoff = Math.min(baseMs + Math.floor(attempts / 15) * 200, visible ? 2200 : 10_000);
        timeoutId = setTimeout(poll, backoff);
      } catch (err) {
        if (err?.name === 'CanceledError' || err?.name === 'AbortError' || err?.code === 'ERR_CANCELED') {
          return;
        }
        consecutiveErrors += 1;
        const msg = err?.response?.data?.error || err?.message || 'Import status unavailable';
        setError(msg);
        // If status endpoint fails repeatedly, surface terminal failure so overlay/UI never hangs.
        if (consecutiveErrors >= 3) {
          const failedStatus = {
            status: 'failed',
            error: `Unable to fetch import status: ${msg}`,
            totalRows: status?.totalRows || 0,
            processedRows: status?.processedRows || 0,
            failedRows: status?.failedRows || 0,
          };
          onStatusChangeRef.current?.(failedStatus);
          onCompleteRef.current?.(failedStatus);
          return;
        }
        timeoutId = setTimeout(poll, visible ? 2500 : 6000);
      }
    };

    poll();

    return () => {
      ac.abort();
      if (timeoutId) clearTimeout(timeoutId);
    };
  }, [jobId, visible]);

  if (!jobId || !status) return null;

  const progress = status.totalRows > 0
    ? Math.round((status.processedRows / status.totalRows) * 100)
    : 0;

  const elapsed = status.startedAt ? (Date.now() - new Date(status.startedAt).getTime()) / 1000 : 0;
  const rate = status.processedRows > 0 && elapsed > 0 ? status.processedRows / elapsed : 0;
  const eta = status.status === 'processing' && rate > 0 && status.totalRows > 0
    ? Math.round((status.totalRows - status.processedRows) / rate)
    : null;

  const formatTime = (sec) => {
    if (sec < 60) return `${Math.round(sec)}s`;
    const m = Math.floor(sec / 60);
    const s = Math.round(sec % 60);
    return `${m}m ${s}s`;
  };

  return (
    <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
      <div className="px-5 py-4 border-b border-slate-100 bg-slate-50/30">
        <h3 className="text-base font-semibold text-slate-800">Import progress</h3>
        <p className="text-xs text-slate-500 mt-0.5">Processing your file</p>
      </div>
      <div className="p-5 space-y-4">
        {(error || status.error) && (
          <div className="text-sm text-red-700 bg-red-50 border border-red-200/80 p-4 rounded-xl">
            {error || status.error}
          </div>
        )}

        <div className="flex justify-between text-sm text-slate-600">
          <span className="font-medium tabular-nums">
            {status.processedRows.toLocaleString()} / {status.totalRows ? status.totalRows.toLocaleString() : '?'} rows
          </span>
          {status.failedRows > 0 && <span className="text-amber-600 font-medium">{status.failedRows} failed</span>}
        </div>

        <div className="w-full bg-slate-200 rounded-full h-2.5 overflow-hidden">
          <div className="bg-indigo-600 h-2.5 rounded-full transition-all duration-300" style={{ width: `${progress}%` }} role="progressbar" aria-valuenow={progress} aria-valuemin={0} aria-valuemax={100} />
        </div>

        {eta != null && status.status === 'processing' && (
          <p className="text-xs text-slate-500">ETA: {formatTime(eta)}</p>
        )}

        {status.status === 'completed' && (
          <p className="text-sm text-emerald-600 font-medium">Completed</p>
        )}
        {status.status === 'cancelled' && (
          <p className="text-sm text-amber-600 font-medium">Cancelled</p>
        )}

        {status.status === 'processing' && (
          <button
            type="button"
            onClick={() => importApi.cancel(jobId).catch(() => {})}
            className="w-full mt-1 px-4 py-2.5 text-sm font-medium border border-slate-200 text-slate-700 rounded-xl hover:bg-slate-50 transition-colors"
          >
            Cancel import
          </button>
        )}
      </div>
    </div>
  );
}
