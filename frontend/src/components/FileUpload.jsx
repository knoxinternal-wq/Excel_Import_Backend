import { useCallback, useState, useRef, forwardRef, useImperativeHandle } from 'react';
import { useDropzone } from 'react-dropzone';
import { Upload, FileSpreadsheet, Download, Loader2, X } from 'lucide-react';
import { importApi } from '../services/api';

const FileUpload = forwardRef(function FileUpload({ onUploadStart, onUploadComplete, onUploadProgress, onUploadError, onUploadAbortReady }, ref) {
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [uploadPercent, setUploadPercent] = useState(0);
  const [error, setError] = useState(null);
  const abortControllerRef = useRef(null);

  const onDrop = useCallback((acceptedFiles) => {
    setError(null);
    if (acceptedFiles.length > 0) {
      const f = acceptedFiles[0];
      const ext = f.name.toLowerCase().split('.').pop();
      if (ext !== 'xlsx' && ext !== 'xls') {
        setError('Only Excel files (.xlsx, .xls) are allowed');
        setFile(null);
        return;
      }
      setFile(f);
    }
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: { 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'], 'application/vnd.ms-excel': ['.xls'] },
    maxFiles: 1,
    disabled: uploading,
  });

  const handleUpload = async () => {
    if (!file) {
      setError('Please select a file first');
      return;
    }
    setError(null);
    abortControllerRef.current = new AbortController();
    const signal = abortControllerRef.current.signal;

    const formData = new FormData();
    formData.append('file', file);

    // Start the HTTP request before React state updates so the browser begins sending immediately.
    const uploadPromise = importApi.upload(
      formData,
      (pct) => {
        setUploadPercent(pct);
        onUploadProgress?.(pct);
      },
      signal,
    );

    setUploading(true);
    setUploadPercent(0);
    onUploadStart?.(file);
    onUploadAbortReady?.(() => abortControllerRef.current?.abort());

    try {
      const { data } = await uploadPromise;
      if (!data?.jobId) {
        throw new Error(data?.error || 'Import job was not created');
      }
      onUploadComplete?.(data.jobId);
    } catch (err) {
      const isCanceled = err?.name === 'CanceledError' || err?.name === 'AbortError' || err?.code === 'ERR_CANCELED';
      const msg = isCanceled ? 'Upload cancelled' : (err.response?.data?.error || err.message);
      setError(msg);
      onUploadError?.({ message: msg, cancelled: isCanceled });
    } finally {
      abortControllerRef.current = null;
      onUploadAbortReady?.(null);
      setUploading(false);
      setUploadPercent(0);
    }
  };

  const handleStartImport = () => {
    if (file) handleUpload();
  };

  const formatSize = (bytes) => {
    if (!bytes) return '-';
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
  };

  useImperativeHandle(ref, () => ({
    clearFile: () => { setFile(null); setError(null); },
  }), []);

  return (
    <div className="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden max-w-md">
      <div className="px-4 py-3 border-b border-slate-100 bg-slate-50/30 flex justify-between items-center gap-3">
        <div className="flex items-center gap-2 min-w-0">
          <div className="w-8 h-8 rounded-lg bg-indigo-100 flex items-center justify-center flex-shrink-0" aria-hidden>
            <Upload className="w-4 h-4 text-indigo-600" />
          </div>
          <div className="min-w-0">
            <h3 className="text-sm font-semibold text-slate-800">Upload sales Excel file</h3>
            <p className="text-xs text-slate-500">.xlsx or .xls, up to 500K rows</p>
          </div>
        </div>
        <a
          href="/api/import/template"
          download="sales_data_template.xlsx"
          className="flex items-center gap-1.5 text-xs text-indigo-600 hover:text-indigo-700 font-medium px-2 py-1.5 rounded-lg hover:bg-indigo-50 transition-colors flex-shrink-0"
        >
          <Download className="w-3.5 h-3.5" aria-hidden />
          Template
        </a>
      </div>

      <div className="p-4 space-y-3">
        <div
          {...getRootProps()}
          className={`border-2 border-dashed rounded-lg p-5 text-center cursor-pointer transition-all duration-200 focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 focus-visible:ring-offset-2 ${
            isDragActive ? 'border-indigo-500 bg-indigo-50/50' : 'border-slate-200 hover:border-indigo-300 hover:bg-slate-50/50'
          }`}
        >
          <input {...getInputProps()} aria-label="Select Excel file" />
          <div className="flex justify-center mb-2">
            <div className={`w-10 h-10 rounded-xl flex items-center justify-center transition-colors ${isDragActive ? 'bg-indigo-100' : 'bg-slate-100'}`}>
              <FileSpreadsheet className={`w-5 h-5 ${isDragActive ? 'text-indigo-600' : 'text-slate-400'}`} aria-hidden />
            </div>
          </div>
          <p className="text-slate-600 text-xs font-medium">
            {isDragActive ? 'Drop the file here' : 'Drag and drop or browse'}
          </p>
        </div>

        {file && (
          <div className="flex items-center gap-2 p-3 bg-slate-50 rounded-lg border border-slate-200/80">
            <FileSpreadsheet className="w-5 h-5 text-emerald-600 flex-shrink-0" aria-hidden />
            <div className="min-w-0 flex-1">
              <p className="text-xs font-medium text-slate-800 truncate">{file.name}</p>
              <p className="text-xs text-slate-500">{formatSize(file.size)}</p>
            </div>
            <button
              type="button"
              onClick={(e) => { e.stopPropagation(); setFile(null); setError(null); }}
              className="flex-shrink-0 p-1.5 rounded-lg text-slate-400 hover:text-red-600 hover:bg-red-50 transition-colors"
              title="Remove file"
              aria-label="Remove file"
            >
              <X className="w-4 h-4" />
            </button>
          </div>
        )}

        {error && (
          <div className="flex items-start gap-2 p-3 rounded-lg bg-red-50 border border-red-200/80 text-red-800 text-xs">
            <span className="flex-shrink-0 mt-0.5 w-3.5 h-3.5 rounded-full bg-red-200 flex items-center justify-center text-red-600 text-[10px] font-bold">!</span>
            <span>{error}</span>
          </div>
        )}

        <button
          onClick={handleStartImport}
          disabled={!file || uploading}
          className="w-full px-3 py-2.5 bg-indigo-600 text-white text-xs font-semibold rounded-lg hover:bg-indigo-700 disabled:opacity-50 disabled:cursor-not-allowed transition-all flex items-center justify-center gap-2 shadow-sm focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 focus-visible:ring-offset-2"
        >
          {uploading ? <><Loader2 className="w-3.5 h-3.5 animate-spin" aria-hidden /> Uploading...</> : 'Upload & start import'}
        </button>
      </div>
    </div>
  );
});

export default FileUpload;
