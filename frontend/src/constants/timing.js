/**
 * Client-side request budgets (axios `timeout`). Tune via Vite env without code changes.
 * - Fast: grids, metadata, auth — default 60s (5s was too tight for cold DB / large COUNT / slow networks).
 * - Filter distinct lists: bounded separately (can be slower on huge columns).
 * - Long: pivot body, export, import, bulk delete — can run much longer.
 */
function parseMs(envVal, fallbackMs) {
  const n = Number(envVal);
  return Number.isFinite(n) && n >= 1000 ? Math.floor(n) : fallbackMs;
}

/** Default 60s; set VITE_API_FAST_TIMEOUT_MS (ms) in .env if you need more (e.g. 120000). */
export const MAX_UI_DATA_LOAD_MS = parseMs(import.meta.env?.VITE_API_FAST_TIMEOUT_MS, 60_000);

/**
 * DISTINCT pivot filter dropdowns (GET + batch POST). Large tables can need >30s on cold DB / Render.
 * Default 3 minutes; must be ≥ server PIVOT_FILTER_SQL_TIMEOUT_MS or the browser aborts first.
 */
export const PIVOT_FILTER_VALUES_TIMEOUT_MS = parseMs(
  import.meta.env?.VITE_PIVOT_FILTER_TIMEOUT_MS,
  180_000,
);

export const LONG_RUNNING_REQUEST_MS = parseMs(
  import.meta.env?.VITE_API_LONG_TIMEOUT_MS,
  15 * 60 * 1000,
);
