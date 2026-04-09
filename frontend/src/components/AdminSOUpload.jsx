import { useState, useEffect, useCallback } from 'react';
import { Loader2, Upload } from 'lucide-react';
import { adminApi } from '../services/api';
import usePersistentState from '../hooks/usePersistentState';

function buildFYPresets({ yearsBack = 10, yearsForward = 10 } = {}) {
  const now = new Date();
  const currentYear = now.getFullYear();
  const start = currentYear - yearsBack;
  const end = currentYear + yearsForward;
  const out = [];
  for (let y = start; y <= end; y += 1) {
    out.push(`${y}-${String(y + 1).slice(-2)}`);
  }
  return out;
}

const FY_PRESETS = buildFYPresets({ yearsBack: 10, yearsForward: 10 });

/** Only these legacy SO master brands are supported for upload. */
const BRAND_OPTIONS = [
  'DON AND JULIO',
  'ITALIAN CHANNEL',
  'RISHAB FABRICS',
  'VERCELLI',
];
const ALLOWED_BRANDS = new Set(BRAND_OPTIONS.map((b) => normalizeBrandKey(b)));
const FY_RE = /^\d{4}-\d{2}$/;

function normalizeBrandKey(b) {
  return String(b ?? '')
    .trim()
    .toUpperCase();
}

function toDateInputValue(v) {
  if (v == null || String(v).trim() === '') return '';
  const raw = String(v).trim();
  // Already yyyy-MM-dd
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  // ISO timestamp
  if (/^\d{4}-\d{2}-\d{2}T/.test(raw)) return raw.slice(0, 10);
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return '';
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function formatHistoryValue(value) {
  if (value == null || value === '') return '-';
  return String(value);
}

/**
 * Upload SO master (PARTY NAME + TYPE OF ORDER) for a brand + FY.
 * @param {{ onNotify?: (toast: { type: string, message: string }) => void }} props
 */
export default function AdminSOUpload({ onNotify, userEmail }) {
  const [brand, setBrand] = usePersistentState(
    'admin_so_master_brand',
    '',
    (v) => typeof v === 'string' && (v.trim() === '' || ALLOWED_BRANDS.has(normalizeBrandKey(v))),
  );
  const [fy, setFy] = usePersistentState(
    'admin_so_master_fy',
    '',
    (v) => typeof v === 'string' && (v.trim() === '' || FY_RE.test(v.trim())),
  );
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [localError, setLocalError] = useState(null);
  const [previewNonce, setPreviewNonce] = useState(0);
  const [preview, setPreview] = useState(null);
  const [uploadPop, setUploadPop] = useState(null); // { type: 'success'|'error', message: string }
  const [history, setHistory] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);

  const [adminSection, setAdminSection] = usePersistentState(
    'admin_so_master_section',
    'upload',
    (v) => v === 'upload' || v === 'edit',
  );

  const [masterTables, setMasterTables] = useState([]);
  const [selectedMasterTable, setSelectedMasterTable] = usePersistentState(
    'admin_so_master_edit_table',
    '',
    (v) => typeof v === 'string',
  );
  const [masterSearchInput, setMasterSearchInput] = useState('');
  const [masterSearch, setMasterSearch] = useState('');
  const [masterPreview, setMasterPreview] = useState({ loading: false, error: null, rows: [], editableColumns: [] });
  const [editDraftByRow, setEditDraftByRow] = useState({});
  const [editSavingRowUuid, setEditSavingRowUuid] = useState(null);
  const [editError, setEditError] = useState(null);

  const [masterEditHistory, setMasterEditHistory] = useState([]);
  const [masterEditHistoryLoading, setMasterEditHistoryLoading] = useState(false);

  const loadHistory = useCallback(async () => {
    try {
      setHistoryLoading(true);
      const { data } = await adminApi.soMasterHistory(20);
      if (data?.success) setHistory(Array.isArray(data.rows) ? data.rows : []);
    } catch {
      // ignore (history is non-critical)
    } finally {
      setHistoryLoading(false);
    }
  }, []);

  useEffect(() => {
    loadHistory();
  }, [loadHistory]);

  const brandKey = normalizeBrandKey(brand);
  const canSubmit = Boolean(brandKey && String(fy).trim() && file && !uploading);

  const loadMasterEditHistory = useCallback(async () => {
    try {
      setMasterEditHistoryLoading(true);
      if (adminSection !== 'edit') {
        setMasterEditHistory([]);
        return;
      }
      const { data } = await adminApi.masterTableEditHistory({ limit: 80 });
      if (data?.success) setMasterEditHistory(Array.isArray(data.rows) ? data.rows : []);
      else setMasterEditHistory([]);
    } catch {
      setMasterEditHistory([]);
    } finally {
      setMasterEditHistoryLoading(false);
    }
  }, [adminSection]);

  const loadMasterPreview = useCallback(async () => {
    try {
      if (adminSection !== 'edit' || !selectedMasterTable) {
        setMasterPreview({ loading: false, error: null, rows: [], editableColumns: [] });
        return;
      }
      setMasterPreview((s) => ({ ...s, loading: true, error: null }));
      const { data } = await adminApi.masterTablePreview({ table: selectedMasterTable, q: masterSearch, limit: 200 });
      if (!data?.success) {
        setMasterPreview({ loading: false, error: 'Unable to load table preview.', rows: [], editableColumns: [] });
        return;
      }
      const rows = Array.isArray(data.rows) ? data.rows : [];
      const editableColumns = Array.isArray(data.editableColumns) ? data.editableColumns : [];
      setMasterPreview({ loading: false, error: null, rows, editableColumns });

      const draft = {};
      for (const r of rows) {
        const rowId = r?.__row_id;
        if (!rowId) continue;
        draft[rowId] = { ...r };
      }
      setEditDraftByRow(draft);
      setEditError(null);
      setEditSavingRowUuid(null);
    } catch (e) {
      const msg = e?.response?.data?.error || e?.response?.data?.message || e?.message || 'Unable to load table preview.';
      setMasterPreview({ loading: false, error: msg, rows: [], editableColumns: [] });
    }
  }, [adminSection, selectedMasterTable, masterSearch]);

  useEffect(() => {
    const t = setTimeout(() => setMasterSearch(masterSearchInput.trim()), 250);
    return () => clearTimeout(t);
  }, [masterSearchInput]);

  useEffect(() => {
    if (adminSection !== 'edit') return;
    setMasterSearchInput('');
    setMasterSearch('');
    (async () => {
      try {
        const { data } = await adminApi.masterTableOptions();
        const rows = Array.isArray(data?.rows) ? data.rows : [];
        setMasterTables(rows);
        if (selectedMasterTable && !rows.some((x) => x.table === selectedMasterTable)) {
          setSelectedMasterTable('');
        }
      } catch {
        setMasterTables([]);
      }
    })();
  }, [adminSection, selectedMasterTable, setSelectedMasterTable]);

  useEffect(() => {
    if (adminSection !== 'edit') return;
    loadMasterPreview();
    loadMasterEditHistory();
  }, [adminSection, selectedMasterTable, masterSearch, previewNonce, loadMasterPreview, loadMasterEditHistory]);

  const handleFile = useCallback((e) => {
    const f = e.target.files?.[0];
    setFile(f || null);
    setLocalError(null);
  }, []);

  const handleSubmit = useCallback(
    async (e) => {
      e.preventDefault();
      if (!canSubmit) return;
      setUploading(true);
      setLocalError(null);
      setUploadPop(null);
      const fd = new FormData();
      fd.append('file', file);
      fd.append('brand', brandKey);
      fd.append('fy', String(fy).trim());
      try {
        const { data } = await adminApi.importSoMaster(fd);
        if (data?.success) {
          onNotify?.({
            type: 'success',
            message: `SO master saved: ${data.inserted ?? 0} row(s).`,
          });
          setUploadPop({
            type: 'success',
            message: `Upload successful: ${data.inserted ?? 0} row(s) saved.`,
          });
          setFile(null);
          const input = document.getElementById('admin-so-master-file');
          if (input) input.value = '';
          setPreview(null);
          setPreviewNonce((n) => n + 1);
        } else {
          const msg = data?.error || 'Upload failed.';
          setLocalError(msg);
          onNotify?.({ type: 'error', message: msg });
          setUploadPop({ type: 'error', message: msg });
        }
      } catch (err) {
        const msg =
          err?.response?.data?.error || err?.response?.data?.message || err?.message || 'Upload failed.';
        setLocalError(msg);
        onNotify?.({ type: 'error', message: msg });
        setUploadPop({ type: 'error', message: msg });
      } finally {
        setUploading(false);
        loadHistory();
      }
    },
    [brandKey, canSubmit, file, fy, onNotify, loadHistory, setBrand, setFy],
  );

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();

    const b = brandKey;
    const y = String(fy || '').trim();

    if (!b || !y) {
      setPreview(null);
      return () => controller.abort();
    }

    setPreview({
      loading: true,
      error: null,
      table: null,
      total: 0,
      rows: [],
    });

    (async () => {
      try {
        const { data } = await adminApi.previewSoMaster(
          { brand: b, fy: y, limit: 20 },
          { signal: controller.signal },
        );
        if (cancelled) return;
        setPreview({
          loading: false,
          error: null,
          table: data?.table ?? null,
          total: Number(data?.total ?? 0),
          rows: Array.isArray(data?.rows) ? data.rows : [],
        });
      } catch (err) {
        if (cancelled) return;
        const msg =
          err?.response?.data?.error ||
          err?.response?.data?.message ||
          err?.message ||
          'Unable to load preview.';
        setPreview({
          loading: false,
          error: msg,
          table: null,
          total: 0,
          rows: [],
        });
      }
    })();

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [brandKey, fy, previewNonce]);

  return (
    <section
      className="rounded-2xl border border-slate-200 bg-white p-5 sm:p-6 shadow-sm"
      aria-labelledby="admin-so-heading"
    >
      <header className="mb-5">
        <h2
          id="admin-so-heading"
          className="text-lg sm:text-xl font-semibold text-slate-900 tracking-tight"
        >
          {adminSection === 'upload' ? 'SO master upload' : 'SO master edit'}
        </h2>
        <p className="mt-1 text-sm text-slate-600 leading-relaxed max-w-[70ch]">
          {adminSection === 'upload' ? (
            <>
              Upload Excel to upsert SO type mapping for the selected{' '}
              <span className="font-medium text-slate-800">Brand</span> and{' '}
              <span className="font-medium text-slate-800">FY</span>.
            </>
          ) : (
            <>
              Select any master table, preview its data, search values, and edit rows with full change history.
            </>
          )}
        </p>
      </header>

      <div className="flex gap-2 mb-4">
        <button
          type="button"
          onClick={() => setAdminSection('upload')}
          className={[
            'flex-1 inline-flex justify-center items-center rounded-lg px-3 py-2 text-sm font-medium transition',
            adminSection === 'upload'
              ? 'bg-slate-900 text-white'
              : 'bg-white text-slate-700 border border-slate-200 hover:bg-slate-50',
          ].join(' ')}
        >
          Master Upload
        </button>
        <button
          type="button"
          onClick={() => setAdminSection('edit')}
          className={[
            'flex-1 inline-flex justify-center items-center rounded-lg px-3 py-2 text-sm font-medium transition',
            adminSection === 'edit'
              ? 'bg-slate-900 text-white'
              : 'bg-white text-slate-700 border border-slate-200 hover:bg-slate-50',
          ].join(' ')}
        >
          Master Edit
        </button>
      </div>

      {adminSection === 'upload' ? (
        <>
      <form onSubmit={handleSubmit} className="grid grid-cols-1 md:grid-cols-2 gap-4 max-w-3xl">
        <div className="md:col-span-1">
          <label htmlFor="admin-so-brand" className="block text-sm font-medium text-slate-700 mb-1">
            Brand
          </label>
          <select
            id="admin-so-brand"
            value={brandKey}
            onChange={(e) => setBrand(normalizeBrandKey(e.target.value))}
            className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
          >
            <option value="">Select brand</option>
            {BRAND_OPTIONS.map((b) => (
              <option key={normalizeBrandKey(b)} value={normalizeBrandKey(b)}>
                {normalizeBrandKey(b)}
              </option>
            ))}
          </select>
        </div>

        <div className="md:col-span-1">
          <label htmlFor="admin-so-fy" className="block text-sm font-medium text-slate-700 mb-1">
            FY
          </label>
          <input
            id="admin-so-fy"
            type="text"
            list="admin-fy-presets"
            value={fy}
            onChange={(e) => setFy(e.target.value)}
            placeholder="e.g. 2025-26"
            className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
          />
          <datalist id="admin-fy-presets">
            {FY_PRESETS.map((x) => (
              <option key={x} value={x} />
            ))}
          </datalist>
          <p className="mt-1 text-xs text-slate-500">Must match the FY derived from bill date during import (Apr–Mar).</p>
        </div>

        <div className="md:col-span-2">
          <label htmlFor="admin-so-master-file" className="block text-sm font-medium text-slate-700 mb-1">
            File (.xlsx)
          </label>
          <div className="flex items-center gap-3 rounded-lg border border-slate-200 bg-white px-3 py-2">
            <input
              id="admin-so-master-file"
              type="file"
              accept=".xlsx,.xls"
              onChange={handleFile}
              disabled={uploading}
              className="sr-only"
            />
            <label
              htmlFor="admin-so-master-file"
              className={[
                'inline-flex items-center justify-center rounded-lg px-4 py-2 text-sm font-medium transition',
                uploading ? 'bg-slate-900/40 text-white cursor-not-allowed' : 'bg-slate-900 text-white hover:bg-slate-800',
              ].join(' ')}
            >
              Choose file
            </label>

            <div className="min-w-0 flex-1">
              <p
                className="text-sm text-slate-700 truncate"
                title={file?.name || ''}
              >
                {file?.name ? file.name : 'No file chosen'}
              </p>
              <p className="text-xs text-slate-500">Only Excel files (.xlsx, .xls) are allowed.</p>
            </div>
          </div>
        </div>

        {localError && (
          <p className="md:col-span-2 text-sm text-red-600" role="alert">
            {localError}
          </p>
        )}

        <div className="md:col-span-2 flex justify-end">
          <button
            type="submit"
            disabled={!canSubmit}
            className="inline-flex items-center justify-center gap-2 rounded-xl bg-slate-900 px-4 py-2.5 text-sm font-medium text-white shadow-sm transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {uploading ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
                Uploading…
              </>
            ) : (
              <>
                <Upload className="h-4 w-4" aria-hidden />
                Upload
              </>
            )}
          </button>
        </div>
      </form>

      {uploadPop && (
        <div
          className={[
            'mt-5 rounded-xl border px-4 py-3',
            uploadPop.type === 'success'
              ? 'border-green-200 bg-green-50 text-green-900'
              : 'border-red-200 bg-red-50 text-red-900',
          ].join(' ')}
          role="status"
          aria-live="polite"
        >
          <div className="flex items-start justify-between gap-3">
            <p className="text-sm font-medium">{uploadPop.message}</p>
            <button
              type="button"
              onClick={() => setUploadPop(null)}
              className="text-xs font-medium text-inherit opacity-70 hover:opacity-100 underline underline-offset-2"
            >
              Dismiss
            </button>
          </div>
        </div>
      )}

      {preview && (
        <div className="mt-6 rounded-xl border border-slate-200 bg-slate-50 p-4">
          <div className="mb-1 flex items-center justify-between gap-3">
            <p className="text-sm font-semibold text-slate-900">Preview</p>
            <button
              type="button"
              onClick={() => setPreview(null)}
              className="inline-flex items-center justify-center rounded-lg border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-800 hover:bg-slate-50"
            >
              Clear Preview
            </button>
          </div>
          <p className="text-xs text-slate-600">
            Brand <span className="font-medium">{brandKey}</span> • FY <span className="font-medium">{String(fy).trim()}</span>
            {preview.table ? (
              <>
                {' '}
                → <span className="font-medium">{preview.table}</span>
              </>
            ) : null}
          </p>

          {preview.error ? (
            <p className="text-sm text-red-600" role="alert">
              {preview.error}
            </p>
          ) : preview.loading ? (
            <div className="flex items-center gap-2 text-sm text-slate-600">
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
              Loading preview…
            </div>
          ) : preview.rows.length === 0 ? (
            <p className="text-sm text-slate-600">No existing SO master rows found for this Brand + FY.</p>
          ) : (
            <>
              <p className="text-xs text-slate-600 mb-2">
                Showing {preview.rows.length} of {preview.total} row(s).
              </p>
              <div className="overflow-auto rounded-lg border border-slate-200 bg-white">
                <table className="min-w-[520px] w-full text-left text-sm">
                  <thead className="bg-slate-100 text-xs uppercase tracking-wide text-slate-600">
                    <tr>
                      <th className="px-3 py-2">PARTY NAME</th>
                      <th className="px-3 py-2">SO AGENT NAME</th>
                      <th className="px-3 py-2">BRANCH</th>
                      <th className="px-3 py-2">COMPANY NAME</th>
                      <th className="px-3 py-2">SO ORDER NO.</th>
                      <th className="px-3 py-2">SO ORDER DATE</th>
                      <th className="px-3 py-2">TYPE OF ORDER</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-200">
                    {preview.rows.map((r, idx) => (
                      <tr key={`${r.party_name}-${idx}`}>
                        <td className="px-3 py-2 font-medium text-slate-900">{r.party_name}</td>
                        <td className="px-3 py-2 text-slate-700">{r.so_agent_name || ''}</td>
                        <td className="px-3 py-2 text-slate-700">{r.branch || ''}</td>
                        <td className="px-3 py-2 text-slate-700">{r.company_name || ''}</td>
                        <td className="px-3 py-2 text-slate-700">{r.so_order_no || ''}</td>
                        <td className="px-3 py-2 text-slate-700">{r.so_order_date || ''}</td>
                        <td className="px-3 py-2 text-slate-700">{r.type_of_order}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      )}

      <div className="mt-6">
        <div className="flex items-center justify-between mb-2">
          <p className="text-sm font-semibold text-slate-900">Upload history</p>
          {historyLoading ? (
            <div className="flex items-center gap-2 text-xs text-slate-500">
              <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
              Loading…
            </div>
          ) : null}
        </div>

        <div className="overflow-auto rounded-xl border border-slate-200 bg-white">
          {history.length === 0 ? (
            <div className="p-4 text-sm text-slate-600">No admin uploads yet.</div>
          ) : (
            <table className="min-w-[720px] w-full text-left text-sm">
              <thead className="bg-slate-100 text-xs uppercase tracking-wide text-slate-600">
                <tr>
                  <th className="px-3 py-2">FILE</th>
                  <th className="px-3 py-2">BRAND</th>
                  <th className="px-3 py-2">FY</th>
                  <th className="px-3 py-2">STATUS</th>
                  <th className="px-3 py-2">ROWS</th>
                  <th className="px-3 py-2">TIME</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-200">
                {history.map((h, idx) => (
                  <tr key={`${h.createdAt || idx}-${h.filename || idx}`}>
                    <td className="px-3 py-2 font-medium text-slate-900">{h.filename || ''}</td>
                    <td className="px-3 py-2 text-slate-700">{h.brand || ''}</td>
                    <td className="px-3 py-2 text-slate-700">{h.fy || ''}</td>
                    <td className="px-3 py-2 text-slate-700">
                      <span
                        className={[
                          'inline-flex px-2 py-1 rounded-full text-xs font-medium',
                          h.status === 'success' ? 'bg-green-50 text-green-900' : 'bg-red-50 text-red-900',
                        ].join(' ')}
                      >
                        {h.status}
                      </span>
                    </td>
                    <td className="px-3 py-2 text-slate-700">{h.insertedRows ?? 0}</td>
                    <td className="px-3 py-2 text-slate-600">
                      {h.createdAt ? new Date(h.createdAt).toLocaleString() : ''}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
        </>
      ) : (
        <div className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 max-w-4xl">
            <div>
              <label htmlFor="admin-master-table" className="block text-sm font-medium text-slate-700 mb-1">
                Master table
              </label>
              <select
                id="admin-master-table"
                value={selectedMasterTable}
                onChange={(e) => setSelectedMasterTable(e.target.value)}
                className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
              >
                <option value="">No table selected</option>
                {masterTables.map((t) => (
                  <option key={t.table} value={t.table}>
                    {t.label || t.table}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label htmlFor="admin-master-search" className="block text-sm font-medium text-slate-700 mb-1">
                Search in selected table
              </label>
              <input
                id="admin-master-search"
                type="text"
                value={masterSearchInput}
                onChange={(e) => setMasterSearchInput(e.target.value)}
                placeholder="Type any value (party, brand, region, etc.)"
                className="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
              />
              <p className="mt-1 text-xs text-slate-500">Search is fast and auto-applies while typing.</p>
            </div>
          </div>

          {editError && (
            <p className="text-sm text-red-600" role="alert">
              {editError}
            </p>
          )}

          <div className="rounded-xl border border-slate-200 bg-white p-4">
            <p className="text-sm font-semibold text-slate-900 mb-2">
              Table preview: {selectedMasterTable || '-'}
            </p>

            {masterPreview.loading ? (
              <div className="flex items-center gap-2 text-sm text-slate-600">
                <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
                Loading table data...
              </div>
            ) : masterPreview.error ? (
              <p className="text-sm text-red-600">{masterPreview.error}</p>
            ) : !selectedMasterTable ? (
              <p className="text-sm text-slate-600">Select a table from the dropdown to preview and edit rows.</p>
            ) : masterPreview.rows.length === 0 ? (
              <p className="text-sm text-slate-600">No rows found in this table.</p>
            ) : (
              <div className="overflow-auto rounded-lg border border-slate-200 bg-white">
                <table className="min-w-[1400px] w-full text-left text-sm table-auto">
                  <thead className="bg-slate-100 text-xs uppercase tracking-wide text-slate-600">
                    <tr>
                      {Object.keys(masterPreview.rows[0] || {})
                        .filter((col) => !['uuid', 'created_at', '__row_id'].includes(col))
                        .map((col) => (
                          <th key={col} className="px-3 py-2 align-top whitespace-nowrap">{col}</th>
                        ))}
                      <th className="px-3 py-2 text-right align-top whitespace-nowrap">Action</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-200">
                    {masterPreview.rows.map((r) => (
                      <tr key={r.__row_id}>
                        {(() => {
                          const draftForRow = editDraftByRow?.[r.__row_id] || {};
                          const changedUpdates = {};
                          for (const col of masterPreview.editableColumns || []) {
                            const nextVal = draftForRow[col];
                            const prevVal = r[col];
                            if (String(nextVal ?? '') !== String(prevVal ?? '')) {
                              changedUpdates[col] = nextVal;
                            }
                          }
                          const hasChanges = Object.keys(changedUpdates).length > 0;
                          return (
                            <>
                              {Object.keys(masterPreview.rows[0] || {})
                                .filter((col) => !['uuid', 'created_at', '__row_id'].includes(col))
                                .map((col) => {
                                  const canEdit = masterPreview.editableColumns.includes(col);
                                  const isDateField = col === 'so_order_date' || col.endsWith('_date');
                                  const rawValue = editDraftByRow?.[r.__row_id]?.[col] ?? '';
                                  const value = isDateField ? toDateInputValue(rawValue) : rawValue;
                                  const dynamicWidth = `${Math.min(560, Math.max(180, String(value ?? '').length * 8))}px`;
                                  if (!canEdit) {
                                    return (
                                      <td
                                        key={`${r.__row_id}-${col}`}
                                        className="px-3 py-2 text-slate-700 align-top whitespace-pre-wrap break-words"
                                        style={{ minWidth: dynamicWidth }}
                                      >
                                        {String(r[col] ?? '')}
                                      </td>
                                    );
                                  }
                                  return (
                                    <td key={`${r.__row_id}-${col}`} className="px-3 py-2 align-top" style={{ minWidth: dynamicWidth }}>
                                      <input
                                        type={isDateField ? 'date' : 'text'}
                                        className="w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5 text-sm text-slate-900 shadow-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                                        value={String(value ?? '')}
                                        onChange={(e) => {
                                          const v = e.target.value;
                                          setEditDraftByRow((prev) => ({
                                            ...prev,
                                            [r.__row_id]: {
                                              ...(prev[r.__row_id] || {}),
                                              [col]: v,
                                            },
                                          }));
                                        }}
                                      />
                                    </td>
                                  );
                                })}
                              <td className="px-3 py-2 text-right align-top whitespace-nowrap">
                                <button
                                  type="button"
                                  disabled={editSavingRowUuid === r.__row_id || !hasChanges}
                                  onClick={async () => {
                                    setEditError(null);
                                    setEditSavingRowUuid(r.__row_id);
                                    try {
                                      await adminApi.editMasterTableRow({
                                        table: selectedMasterTable,
                                        row_id: r.__row_id,
                                        updates: changedUpdates,
                                        edited_by: userEmail || 'unknown',
                                      });
                                      setPreviewNonce((n) => n + 1);
                                    } catch (e) {
                                      const msg =
                                        e?.response?.data?.error ||
                                        e?.response?.data?.message ||
                                        e?.message ||
                                        'Update failed.';
                                      setEditError(msg);
                                    } finally {
                                      setEditSavingRowUuid(null);
                                    }
                                  }}
                                  className="inline-flex items-center justify-center rounded-lg border border-slate-300 bg-white px-3 py-2 text-xs font-medium text-slate-800 hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed"
                                >
                                  {editSavingRowUuid === r.__row_id ? (
                                    <span className="inline-flex items-center gap-2">
                                      <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
                                      Saving...
                                    </span>
                                  ) : (
                                    'Save row'
                                  )}
                                </button>
                              </td>
                            </>
                          );
                        })()}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          <div className="rounded-xl border border-slate-200 bg-white p-4">
            <div className="flex items-center justify-between gap-3 mb-2">
              <p className="text-sm font-semibold text-slate-900">Row edit history</p>
              <p className="text-xs text-slate-500">Showing all master tables</p>
            </div>

            {masterEditHistoryLoading ? (
              <div className="flex items-center gap-2 text-sm text-slate-600">
                <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
                Loading history...
              </div>
            ) : masterEditHistory.length === 0 ? (
              <p className="text-sm text-slate-600">No row edits yet.</p>
            ) : (
              <div className="overflow-auto rounded-lg border border-slate-200 bg-white">
                <table className="min-w-[1200px] w-full text-left text-sm">
                  <thead className="bg-slate-100 text-xs uppercase tracking-wide text-slate-600">
                    <tr>
                      <th className="px-3 py-2">Table</th>
                      <th className="px-3 py-2">Edited row</th>
                      <th className="px-3 py-2">Changed column</th>
                      <th className="px-3 py-2">Previous</th>
                      <th className="px-3 py-2">After</th>
                      <th className="px-3 py-2">Edited By</th>
                      <th className="px-3 py-2 text-right">When</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-200">
                    {masterEditHistory.map((h, idx) => (
                      <tr key={`${h.master_table}-${h.row_uuid}-${h.column_name}-${h.created_at}-${idx}`}>
                        <td className="px-3 py-2 text-slate-800 align-top whitespace-nowrap">{h.master_table}</td>
                        <td className="px-3 py-2 text-slate-700 align-top min-w-[420px]">
                          {h.row_data && typeof h.row_data === 'object' ? (
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-x-4 gap-y-1">
                              {Object.entries(h.row_data).map(([key, value]) => (
                                <div key={`${h.row_uuid}-${key}`} className="text-xs text-slate-700 break-words">
                                  <span className="font-medium text-slate-900">{key}:</span> {formatHistoryValue(value)}
                                </div>
                              ))}
                            </div>
                          ) : (
                            <span className="text-xs text-slate-500">Edited row data not available.</span>
                          )}
                        </td>
                        <td className="px-3 py-2 text-slate-800 align-top whitespace-nowrap">{h.column_name}</td>
                        <td className="px-3 py-2 text-slate-700 align-top">{formatHistoryValue(h.previous_value)}</td>
                        <td className="px-3 py-2 text-slate-700 align-top">{formatHistoryValue(h.new_value)}</td>
                        <td className="px-3 py-2 text-slate-700 align-top whitespace-nowrap">{h.edited_by ?? ''}</td>
                        <td className="px-3 py-2 text-right text-slate-600 align-top whitespace-nowrap">
                          {h.created_at ? new Date(h.created_at).toLocaleString() : ''}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}
    </section>
  );
}
