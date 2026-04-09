import { useEffect, useMemo } from 'react';
import { Loader2 } from 'lucide-react';

const SUCCESS_DISPLAY_MS = 1000;

function formatETA(sec) {
  if (sec == null || sec < 0) return null;
  if (sec < 60) return `${Math.round(sec)}s`;
  const m = Math.floor(sec / 60);
  const s = Math.round(sec % 60);
  return `${m}m ${s}s`;
}

export default function UploadOverlay({
  phase = 'idle',
  uploadProgress = 0,
  fileName = '',
  importStatus = null,
  onCancel,
  cancelPending = false,
  onExitComplete,
}) {
  useEffect(() => {
    if (phase !== 'exiting') return undefined;
    const t = setTimeout(() => onExitComplete?.(), SUCCESS_DISPLAY_MS);
    return () => clearTimeout(t);
  }, [phase, onExitComplete]);

  const showSuccess = phase === 'exiting';

  // Hooks must run every render — never return early before useMemo (fixes "more hooks than previous render").
  const { rowProgressPct, eta } = useMemo(() => {
    if (phase === 'idle' || !importStatus || phase !== 'importing') return { rowProgressPct: 0, eta: null };
    const total = importStatus.totalRows || 0;
    const processed = importStatus.processedRows || 0;
    const pct = total > 0 ? Math.round((processed / total) * 100) : processed > 0 ? 2 : 0;
    const start = importStatus.startedAt ? new Date(importStatus.startedAt).getTime() : 0;
    const elapsed = start ? (Date.now() - start) / 1000 : 0;
    const rate = processed > 0 && elapsed > 0 ? processed / elapsed : 0;
    const estimated = rate > 0 && total > 0 ? Math.round((total - processed) / rate) : null;
    return { rowProgressPct: pct, eta: estimated };
  }, [phase, importStatus?.processedRows, importStatus?.totalRows, importStatus?.startedAt]);

  const displayProgress = useMemo(() => {
    if (showSuccess) return 100;
    if (phase === 'uploading') return Math.max(2, Math.round(uploadProgress * 0.15));
    if (phase === 'importing') return Math.max(2, Math.round(15 + rowProgressPct * 0.85));
    return 2;
  }, [showSuccess, phase, uploadProgress, rowProgressPct]);

  if (phase === 'idle') return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/80 backdrop-blur-sm">
      <div className="bg-white rounded-2xl shadow-2xl p-8 max-w-md w-full">
        <div className="flex items-center gap-4 mb-6">
          <div className={`w-14 h-14 rounded-full flex items-center justify-center ${showSuccess ? 'bg-emerald-100' : 'bg-indigo-50'}`}>
            {showSuccess ? (
              <svg className="w-8 h-8 text-emerald-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2.5} d="M5 13l4 4L19 7" />
              </svg>
            ) : (
              <Loader2 className="w-8 h-8 text-indigo-600 animate-spin" aria-hidden />
            )}
          </div>
          <div className="min-w-0">
            <h3 className="text-lg font-semibold text-slate-800">
              {showSuccess ? 'Import complete' : 'Import in progress'}
            </h3>
            <p className="text-sm text-slate-500 mt-0.5 truncate" title={phase === 'uploading' ? fileName : undefined}>
              {showSuccess
                ? 'Table refreshed'
                : phase === 'uploading'
                  ? (fileName ? `${fileName} · ${Math.round(uploadProgress)}% sent` : 'Sending file…')
                  : `${importStatus?.processedRows?.toLocaleString() ?? 0} / ${importStatus?.totalRows?.toLocaleString() ?? '…'} rows`}
            </p>
          </div>
        </div>

        {!showSuccess && (
          <div className="mb-6">
            <div className="h-2.5 bg-slate-200 rounded-full overflow-hidden">
              <div
                className="h-full bg-blue-600 rounded-full transition-all duration-300"
                style={{ width: `${Math.min(100, Math.max(displayProgress, 2))}%` }}
              />
            </div>
            <p className="text-xs text-slate-500 mt-2">
              {phase === 'uploading'
                ? 'Uploading to server…'
                : eta != null
                  ? `ETA: ${formatETA(eta)}`
                  : importStatus?.totalRows
                    ? 'Writing rows to database…'
                    : 'Preparing import…'}
            </p>
          </div>
        )}

        {!showSuccess && importStatus?.failedRows > 0 && (
          <p className="text-amber-600 text-sm mb-4">
            {importStatus.failedRows.toLocaleString()} rows failed (see Import history)
          </p>
        )}

        {!showSuccess && onCancel && (
          <button
            type="button"
            disabled={cancelPending}
            onClick={(e) => { e.stopPropagation(); e.preventDefault(); onCancel(); }}
            className="w-full py-3 text-sm font-medium text-slate-700 border-2 border-slate-200 rounded-xl hover:bg-slate-50 hover:border-slate-300 transition-all duration-200 disabled:opacity-60 disabled:cursor-not-allowed"
          >
            {cancelPending ? 'Cancelling...' : (phase === 'uploading' ? 'Cancel Upload' : 'Cancel Import')}
          </button>
        )}
      </div>
    </div>
  );
}
