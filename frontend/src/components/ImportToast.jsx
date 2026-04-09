import { useEffect } from 'react';
import { X, CheckCircle, AlertCircle, Info } from 'lucide-react';

const AUTO_DISMISS_MS = 6000;

/**
 * Fixed popup for import success / error / cancel — no full-page UI.
 */
export default function ImportToast({ toast, onDismiss }) {
  useEffect(() => {
    if (!toast) return undefined;
    const t = setTimeout(() => onDismiss?.(), AUTO_DISMISS_MS);
    return () => clearTimeout(t);
  }, [toast, onDismiss]);

  if (!toast) return null;

  const { type, message } = toast;
  const isSuccess = type === 'success';
  const isCancelled = type === 'cancelled';

  const styles = isSuccess
    ? 'bg-emerald-900 text-emerald-50 border-emerald-700/50'
    : isCancelled
      ? 'bg-amber-900 text-amber-50 border-amber-700/50'
      : 'bg-red-900 text-red-50 border-red-700/50';

  const Icon = isSuccess ? CheckCircle : isCancelled ? Info : AlertCircle;

  return (
    <div
      className="fixed top-4 right-4 left-4 sm:left-auto z-[100] flex justify-end pointer-events-none"
      role="status"
      aria-live="polite"
    >
      <div
        className={[
          'pointer-events-auto flex items-start gap-3 max-w-md w-full sm:w-auto min-w-[min(100%,20rem)]',
          'rounded-xl border shadow-lg px-4 py-3 text-sm font-medium',
          styles,
        ].join(' ')}
      >
        <Icon className="w-5 h-5 flex-shrink-0 mt-0.5 opacity-90" aria-hidden />
        <p className="flex-1 leading-snug">{message}</p>
        <button
          type="button"
          onClick={() => onDismiss?.()}
          className="flex-shrink-0 p-1 rounded-lg hover:bg-white/10 focus:outline-none focus-visible:ring-2 focus-visible:ring-white/50"
          aria-label="Dismiss"
        >
          <X className="w-4 h-4" />
        </button>
      </div>
    </div>
  );
}
