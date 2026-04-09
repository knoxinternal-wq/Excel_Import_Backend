import { useState, useCallback, useRef, useEffect, lazy, Suspense } from 'react';
import { Upload, Table2, BarChart3, Shield, LogOut } from 'lucide-react';
import UploadOverlay from '../components/UploadOverlay';
import ImportToast from '../components/ImportToast';
import Sidebar from '../components/Sidebar';
import FileUpload from '../components/FileUpload';
import VirtualizedTable from '../components/VirtualizedTable';
import DeleteByDate from '../components/DeleteByDate';
import { importApi, dataApi } from '../services/api';
import usePersistentState from '../hooks/usePersistentState';
const ImportProgress = lazy(() => import('../components/ImportProgress'));
const ImportHistory = lazy(() => import('../components/ImportHistory'));
const PivotReport = lazy(() => import('../components/pivot/PivotReport'));
const AdminSOUpload = lazy(() => import('../components/AdminSOUpload'));

const TABS = [
  { id: 'import', label: 'Import', icon: Upload },
  { id: 'data', label: 'Data', icon: Table2 },
  { id: 'report', label: 'Report', icon: BarChart3 },
  { id: 'admin', label: 'Admin', icon: Shield },
];

export default function Dashboard({ user, onLogout }) {
  const DEFAULT_TAB = 'data';
  const STORAGE_KEY = 'dashboard_active_tab';
  const [activeTab, setActiveTab] = usePersistentState(
    STORAGE_KEY,
    DEFAULT_TAB,
    (v) => typeof v === 'string' && TABS.some((t) => t.id === v),
  );
  const [activeJobId, setActiveJobId] = useState(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [uploadPhase, setUploadPhase] = useState('idle');
  const [uploadProgress, setUploadProgress] = useState(0);
  const [importStatus, setImportStatus] = useState(null);
  const [uploadFileName, setUploadFileName] = useState('');
  const [dataTableBootstrap, setDataTableBootstrap] = useState(null);
  const [importToast, setImportToast] = useState(null);
  const [cancelPending, setCancelPending] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [mountedTabs, setMountedTabs] = useState(() => ({
    import: DEFAULT_TAB === 'import',
    data: DEFAULT_TAB === 'data',
    report: DEFAULT_TAB === 'report',
    admin: DEFAULT_TAB === 'admin',
  }));
  const uploadAbortRef = useRef(null);
  const fileUploadRef = useRef(null);
  const importWatchdogRef = useRef(null);

  useEffect(() => {
    setMountedTabs((prev) => (prev[activeTab] ? prev : { ...prev, [activeTab]: true }));
  }, [activeTab]);

  // Warm lazy chunks so first tab switch is faster.
  useEffect(() => {
    const t = setTimeout(() => {
      import('../components/ImportProgress');
      import('../components/ImportHistory');
      import('../components/pivot/PivotReport');
      import('../components/AdminSOUpload');
    }, 300);
    return () => clearTimeout(t);
  }, []);

  const dismissImportToast = useCallback(() => setImportToast(null), []);

  const handleUploadStart = useCallback((file) => {
    setActiveJobId(null);
    setUploadPhase('uploading');
    setUploadProgress(0);
    setUploadFileName(file?.name ?? '');
    setImportToast(null);
    setCancelPending(false);
    uploadAbortRef.current = null;
  }, []);

  const handleUploadAbortReady = useCallback((abortFn) => {
    uploadAbortRef.current = abortFn;
  }, []);

  const handleUploadProgress = useCallback((pct) => setUploadProgress(pct), []);

  const handleUploadError = useCallback((detail) => {
    setUploadPhase('idle');
    setUploadProgress(0);
    if (detail?.cancelled) return;
    setImportToast({ type: 'error', message: detail?.message || 'Upload failed.' });
  }, []);

  const handleUploadComplete = useCallback((jobId) => {
    if (!jobId) {
      setUploadPhase('idle');
      setImportToast({ type: 'error', message: 'Import unsuccessful: missing job id.' });
      return;
    }
    setActiveJobId(jobId);
    setUploadPhase('importing');
    setUploadProgress(100);
    // So overlay/watchdog see a status before lazy ImportProgress loads + first poll (avoids false "timeout" at 12s).
    setImportStatus({ status: 'queued', processedRows: 0, totalRows: 0, jobId });
  }, []);

  const handleImportComplete = useCallback(async (finalStatus) => {
    const finalState = String(finalStatus?.status || '').toLowerCase();
    setActiveJobId(null);
    setImportStatus(finalStatus || null);
    setCancelPending(false);

    if (finalState !== 'completed') {
      setUploadPhase('idle');
      if (finalState === 'cancelled') {
        setImportToast({ type: 'cancelled', message: 'Import cancelled.' });
      } else {
        setImportToast({
          type: 'error',
          message: `Import unsuccessful${finalStatus?.error ? `: ${finalStatus.error}` : '.'}`,
        });
      }
      return;
    }

    setUploadPhase('exiting');

    let bootstrap = null;
    try {
      const { data: res } = await dataApi.fetch({
        page: 1,
        limit: 100,
        includeTotal: 1,
        sortBy: 'id',
        sortOrder: 'desc',
      });
      bootstrap = res;
    } catch {
      bootstrap = null;
    }

    setRefreshKey((k) => k + 1);
    setDataTableBootstrap(bootstrap);
    setImportToast({ type: 'success', message: 'Import successful. Table refreshed.' });
    fileUploadRef.current?.clearFile?.();
    setActiveTab('data');
  }, []);

  const handleOverlayExit = useCallback(() => {
    setUploadPhase('idle');
    setImportStatus(null);
    setDataTableBootstrap(null);
  }, []);

  useEffect(() => {
    if (uploadPhase !== 'importing') {
      if (importWatchdogRef.current) {
        clearTimeout(importWatchdogRef.current);
        importWatchdogRef.current = null;
      }
      return;
    }
    if (importStatus?.status) {
      if (importWatchdogRef.current) {
        clearTimeout(importWatchdogRef.current);
        importWatchdogRef.current = null;
      }
      return;
    }
    // Last resort if polling never returns a terminal state (network down, etc.). Must exceed lazy chunk + slow first poll.
    importWatchdogRef.current = setTimeout(() => {
      setUploadPhase('idle');
      setActiveJobId(null);
      setImportToast({
        type: 'error',
        message: 'Import unsuccessful: no status updates from server for a long time. Check network and backend logs.',
      });
    }, 20 * 60 * 1000);
    return () => {
      if (importWatchdogRef.current) {
        clearTimeout(importWatchdogRef.current);
        importWatchdogRef.current = null;
      }
    };
  }, [uploadPhase, importStatus]);

  const handleCancelImport = useCallback(() => {
    if (!activeJobId) {
      setUploadPhase('idle');
      setImportStatus(null);
      setCancelPending(false);
      return;
    }
    if (cancelPending) return;
    setCancelPending(true);
    importApi.cancel(activeJobId)
      .then(() => setImportToast({ type: 'cancelled', message: 'Cancel requested. Stopping import…' }))
      .catch((e) => {
        setCancelPending(false);
        setImportToast({
          type: 'error',
          message: `Unable to cancel import${e?.response?.data?.error ? `: ${e.response.data.error}` : '.'}`,
        });
      });
  }, [activeJobId, cancelPending]);

  const handleOverlayCancel = useCallback(() => {
    if (uploadPhase === 'uploading') {
      if (uploadAbortRef.current) {
        uploadAbortRef.current(); // triggers onUploadError → handleUploadError → phase idle
      } else {
        handleCancelImport();
      }
    } else if (activeJobId) {
      handleCancelImport();
    } else {
      handleCancelImport();
    }
  }, [activeJobId, uploadPhase, handleCancelImport]);

  return (
    <div className="relative min-h-screen bg-slate-50 flex">
      <ImportToast toast={importToast} onDismiss={dismissImportToast} />
      <UploadOverlay
        phase={uploadPhase}
        uploadProgress={uploadProgress}
        fileName={uploadFileName}
        importStatus={importStatus}
        cancelPending={cancelPending}
        onCancel={(uploadPhase === 'uploading' || uploadPhase === 'importing') ? handleOverlayCancel : undefined}
        onExitComplete={handleOverlayExit}
      />
      <Sidebar
        activeTab={activeTab}
        onSelectTab={setActiveTab}
        userEmail={user?.email || 'vishal@rishabworld.com'}
        onLogout={onLogout}
        collapsed={sidebarCollapsed}
        onCollapsedChange={setSidebarCollapsed}
      />

      {/* Mobile — same professional palette */}
      <div className="sm:hidden fixed bottom-0 left-0 right-0 z-40 bg-slate-900 border-t border-slate-800 px-2 py-2 pb-[max(0.5rem,env(safe-area-inset-bottom))] flex justify-around gap-1 shadow-[0_-4px_24px_rgba(15,23,42,0.4)]">
        {TABS.map((tab) => {
          const Icon = tab.icon;
          const isActive = activeTab === tab.id;
          return (
            <button
              key={tab.id}
              type="button"
              onClick={() => setActiveTab(tab.id)}
              className={`flex flex-col items-center justify-center gap-0.5 px-3 min-h-[48px] min-w-[4.5rem] rounded-lg text-xs font-medium touch-manipulation transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-900 ${
                isActive
                  ? 'text-slate-50 bg-slate-800 ring-1 ring-blue-600/40'
                  : 'text-slate-500 hover:text-slate-300'
              }`}
            >
              <Icon className="w-5 h-5" aria-hidden />
              {tab.label}
            </button>
          );
        })}
        <button
          type="button"
          onClick={onLogout}
          className="flex flex-col items-center justify-center gap-0.5 px-3 min-h-[48px] min-w-[4.5rem] rounded-lg text-xs font-medium text-slate-500 hover:text-slate-300 touch-manipulation transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-900"
        >
          <LogOut className="w-5 h-5" aria-hidden />
          Logout
        </button>
      </div>

      <div
        className={[
          'flex-1 flex flex-col min-w-0 min-h-screen sm:min-h-0 transition-[padding] duration-200 ease-out',
          sidebarCollapsed ? 'md:pl-[88px]' : 'md:pl-[260px]',
        ].join(' ')}
      >
        <main
          id="main-content"
          className="relative flex-1 overflow-y-auto px-4 sm:px-6 lg:px-8 pt-5 sm:pt-6 pb-28 sm:pb-10 max-w-[1800px] w-full mx-auto"
          tabIndex={-1}
        >
        {mountedTabs.import && (
          <div className={activeTab === 'import' ? 'space-y-8' : 'hidden'} aria-hidden={activeTab !== 'import'}>
            <section aria-labelledby="import-heading">
              <h2 id="import-heading" className="sr-only">Upload and import</h2>
              <div className="grid grid-cols-1 lg:grid-cols-3 gap-5">
                <div className="lg:col-span-2">
                  <FileUpload
                    ref={fileUploadRef}
                    onUploadStart={handleUploadStart}
                    onUploadComplete={handleUploadComplete}
                    onUploadProgress={handleUploadProgress}
                    onUploadError={handleUploadError}
                    onUploadAbortReady={handleUploadAbortReady}
                  />
                </div>
                <div
                  className={
                    uploadPhase === 'uploading' || uploadPhase === 'importing'
                      ? 'sr-only'
                      : 'min-h-[120px]'
                  }
                  aria-hidden={uploadPhase === 'uploading' || uploadPhase === 'importing'}
                >
                  {activeJobId && (
                    <Suspense fallback={<div className="h-[120px] rounded-xl border border-slate-200 bg-white animate-pulse" />}>
                      <ImportProgress
                        jobId={activeJobId}
                        onComplete={handleImportComplete}
                        onStatusChange={setImportStatus}
                      />
                    </Suspense>
                  )}
                </div>
              </div>
            </section>
            <section aria-labelledby="history-heading-import">
              <h2 id="history-heading-import" className="text-sm font-semibold text-slate-700 mb-3">Import history</h2>
              <Suspense fallback={<div className="h-[220px] rounded-xl border border-slate-200 bg-white animate-pulse" />}>
                <ImportHistory refreshTrigger={refreshKey} isImporting={!!activeJobId} />
              </Suspense>
            </section>
          </div>
        )}

        {mountedTabs.data && (
          <div className={activeTab === 'data' ? 'space-y-6' : 'hidden'} aria-hidden={activeTab !== 'data'}>
            <section aria-labelledby="data-heading" className="space-y-4">
              <h2 id="data-heading" className="sr-only">Sales data table</h2>
              <DeleteByDate
                onDeleted={() => setRefreshKey((k) => k + 1)}
                onNotify={setImportToast}
              />
              <VirtualizedTable
                bootstrap={dataTableBootstrap}
                refreshInterval={activeTab === 'data' ? (activeJobId ? 800 : 30_000) : 0}
                refreshTrigger={refreshKey}
              />
            </section>
          </div>
        )}

        {mountedTabs.report && (
          <div className={activeTab === 'report' ? '' : 'hidden'} aria-hidden={activeTab !== 'report'}>
          <Suspense fallback={<div className="h-[420px] rounded-xl border border-slate-200 bg-white animate-pulse" />}>
            <PivotReport />
          </Suspense>
          </div>
        )}

        {mountedTabs.admin && (
          <div className={activeTab === 'admin' ? 'space-y-6' : 'hidden'} aria-hidden={activeTab !== 'admin'}>
            <Suspense fallback={<div className="h-[280px] rounded-xl border border-slate-200 bg-white animate-pulse" />}>
              <AdminSOUpload onNotify={setImportToast} userEmail={user?.email} />
            </Suspense>
          </div>
        )}
        </main>
      </div>
    </div>
  );
}
