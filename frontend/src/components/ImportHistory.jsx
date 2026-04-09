import { useCallback, useEffect, useState } from 'react';
import { History, Eye, Download, X, Loader2 } from 'lucide-react';
import { historyApi } from '../services/api';
import { formatRequestError } from '../utils/requestError';
import usePageVisibility from '../hooks/usePageVisibility';

const FailedRowsModal = ({ jobId, filename, onClose }) => {
  const [rows, setRows] = useState([]);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    historyApi.getFailedRows(jobId).then(({ data }) => setRows(data)).catch(() => setRows([])).finally(() => setLoading(false));
  }, [jobId]);
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 backdrop-blur-sm p-4" onClick={onClose} role="dialog" aria-modal="true" aria-labelledby="failed-rows-title">
      <div className="bg-white rounded-xl shadow-2xl max-w-4xl w-full max-h-[85vh] overflow-hidden border border-slate-200" onClick={(e) => e.stopPropagation()}>
        <div className="px-6 py-4 border-b border-slate-200 flex justify-between items-center bg-slate-50/50">
          <h3 id="failed-rows-title" className="text-base font-semibold text-slate-800">Failed rows — {filename}</h3>
          <button onClick={onClose} className="p-2 rounded-lg text-slate-500 hover:text-slate-700 hover:bg-slate-100 transition-colors" aria-label="Close">×</button>
        </div>
        <div className="overflow-auto max-h-[calc(85vh-4rem)] p-6">
          {loading ? (
            <div className="flex flex-col items-center justify-center gap-3 py-12" role="status" aria-label="Loading failed rows">
              <Loader2 className="h-8 w-8 animate-spin text-blue-600" aria-hidden />
              <p className="text-slate-600 text-sm font-medium">Loading…</p>
            </div>
          ) : rows.length === 0 ? (
            <p className="text-slate-500 text-sm">No failed rows.</p>
          ) : (
            <table className="w-full text-sm border-collapse">
              <thead>
                <tr className="border-b border-slate-200">
                  <th className="text-left py-3 pr-4 font-semibold text-slate-700">Row #</th>
                  <th className="text-left py-3 pr-4 font-semibold text-slate-700">Error</th>
                  <th className="text-left py-3 font-semibold text-slate-700">Row data</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => (
                  <tr key={i} className="border-b border-slate-100 hover:bg-slate-50/50">
                    <td className="py-2.5 pr-4 text-slate-600 tabular-nums">{r.row_number}</td>
                    <td className="py-2.5 pr-4 text-red-600 text-xs max-w-[220px]">{r.error_message || '—'}</td>
                    <td className="py-2.5 text-slate-700 text-xs break-all">
                      {Array.isArray(r.row_data) ? r.row_data.slice(0, 15).join(' | ') : JSON.stringify(r.row_data || {}).slice(0, 200)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
};

function formatDate(d) {
  if (!d) return '-';
  return new Date(d).toLocaleString();
}

function formatDuration(start, end) {
  if (!start || !end) return '-';
  const ms = new Date(end) - new Date(start);
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${s % 60}s`;
}

export default function ImportHistory({ refreshTrigger, isImporting }) {
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [viewFailedFor, setViewFailedFor] = useState(null);
  const [refreshError, setRefreshError] = useState('');
  const pageVisible = usePageVisibility();

  const fetchHistory = useCallback(() => {
    historyApi
      .list(20)
      .then(({ data }) => {
        setHistory(data);
        setRefreshError('');
      })
      .catch((e) => {
        setRefreshError(formatRequestError(e, 'Could not refresh import history.'));
      })
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    fetchHistory();
  }, [fetchHistory]);

  useEffect(() => {
    if (refreshTrigger != null) fetchHistory();
  }, [refreshTrigger, fetchHistory]);

  useEffect(() => {
    if (!isImporting || !pageVisible) return;
    const iv = setInterval(fetchHistory, 3000);
    return () => clearInterval(iv);
  }, [isImporting, fetchHistory, pageVisible]);

  const handleDownloadFailed = async (jobId) => {
    try {
      const { data } = await historyApi.downloadFailed(jobId);
      const url = URL.createObjectURL(new Blob([data]));
      const a = document.createElement('a');
      a.href = url;
      a.download = `failed_rows_${jobId}.csv`;
      a.click();
      URL.revokeObjectURL(url);
    } catch {
      // Keep silent and non-blocking for failed-row download errors.
    }
  };

  if (loading) {
    return (
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
        <div className="px-5 py-4 border-b border-slate-100 bg-slate-50/30">
          <h3 className="text-base font-semibold text-slate-800">Import history</h3>
        </div>
        <div className="flex flex-col items-center justify-center gap-3 px-5 py-16" role="status" aria-label="Loading import history">
          <Loader2 className="h-10 w-10 animate-spin text-blue-600" aria-hidden />
          <p className="text-sm font-medium text-slate-600">Loading history…</p>
        </div>
      </div>
    );
  }

  return (
    <>
      {viewFailedFor && (
        <FailedRowsModal jobId={viewFailedFor.id} filename={viewFailedFor.filename} onClose={() => setViewFailedFor(null)} />
      )}
      <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
        <div className="px-5 py-4 border-b border-slate-100 bg-slate-50/30">
          <h3 className="text-base font-semibold text-slate-800">Import history</h3>
          <p className="text-xs text-slate-500 mt-0.5">Recent imports and status</p>
          {refreshError && <p className="text-xs text-red-600 mt-1">{refreshError}</p>}
        </div>
        <div className="overflow-x-auto max-h-72 overflow-y-auto">
          {history.length === 0 ? (
            <div className="px-5 py-12 text-center">
              <p className="text-sm font-medium text-slate-600">No imports yet.</p>
              <p className="mt-1 text-xs text-slate-400">Upload a sales Excel file to see import history here.</p>
            </div>
          ) : (
            <table className="w-full text-sm">
              <thead className="bg-slate-50 sticky top-0">
                <tr>
                  <th className="text-left px-4 py-3 font-semibold text-slate-700">Filename</th>
                  <th className="text-left px-4 py-3 font-semibold text-slate-700">Status</th>
                  <th className="text-left px-4 py-3 font-semibold text-slate-700">Reason</th>
                  <th className="text-right px-4 py-3 font-semibold text-slate-700">Processed</th>
                  <th className="text-right px-4 py-3 font-semibold text-slate-700">Failed</th>
                  <th className="text-left px-4 py-3 font-semibold text-slate-700">Started</th>
                  <th className="text-left px-4 py-3 font-semibold text-slate-700">Duration</th>
                  <th className="px-4 py-3 w-28"></th>
                </tr>
              </thead>
              <tbody>
                {history.map((h) => (
                  <tr key={h.id} className="border-b border-slate-100 hover:bg-slate-50/50 transition-colors">
                    <td className="px-4 py-2.5 text-slate-700 truncate max-w-[200px]" title={h.filename}>
                      {h.filename}
                    </td>
                    <td className="px-4 py-2.5">
                      <span
                        className={`inline-flex px-2.5 py-1 rounded-lg text-xs font-medium ${
                          h.status === 'completed'
                            ? 'bg-emerald-100 text-emerald-700'
                            : h.status === 'failed'
                            ? 'bg-red-100 text-red-700'
                            : h.status === 'cancelled'
                            ? 'bg-amber-100 text-amber-700'
                            : 'bg-indigo-100 text-indigo-700'
                        }`}
                      >
                        {h.status}
                      </span>
                    </td>
                    <td className="px-4 py-2.5 text-red-600 text-xs max-w-[240px] truncate" title={h.error_message}>
                      {h.status === 'failed' && h.error_message ? h.error_message : '—'}
                    </td>
                    <td className="px-4 py-2.5 text-right text-slate-600 tabular-nums">{h.processed_rows?.toLocaleString() ?? '—'}</td>
                    <td className="px-4 py-2.5 text-right text-slate-600 tabular-nums">{h.failed_rows?.toLocaleString() ?? '0'}</td>
                    <td className="px-4 py-2.5 text-slate-600 text-xs">{formatDate(h.started_at)}</td>
                    <td className="px-4 py-2.5 text-slate-600 text-xs">{formatDuration(h.started_at, h.completed_at)}</td>
                    <td className="px-4 py-2.5">
                      {h.failed_rows > 0 && (
                        <span className="flex gap-2">
                          <button onClick={() => setViewFailedFor({ id: h.id, filename: h.filename })} className="text-indigo-600 hover:text-indigo-700 text-xs font-medium">
                            View failed
                          </button>
                          <button onClick={() => handleDownloadFailed(h.id)} className="text-indigo-600 hover:text-indigo-700 text-xs font-medium">
                            Download
                          </button>
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </>
  );
}
