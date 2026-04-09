import { MAX_UI_DATA_LOAD_MS } from '../constants/timing';

/**
 * Human-readable API failure (timeouts, validation, network).
 * @param {unknown} err
 * @param {string} [fallback]
 * @param {{ timeoutMs?: number }} [opts] — match the axios timeout used for this call when it differs from MAX_UI_DATA_LOAD_MS
 */
export function formatRequestError(err, fallback = 'Request failed', opts = {}) {
  const timeoutMs = opts.timeoutMs ?? MAX_UI_DATA_LOAD_MS;
  const sec = Math.max(1, Math.round(timeoutMs / 1000));
  const msg = String(err?.message || '');
  if (err?.code === 'ECONNABORTED' || msg.toLowerCase().includes('timeout')) {
    return `Request timed out after ${sec}s. Check your connection or try again.`;
  }
  return err?.response?.data?.error || msg || fallback;
}
