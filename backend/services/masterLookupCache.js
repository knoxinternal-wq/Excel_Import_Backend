/**
 * In-memory TTL cache for master-table maps and row sets.
 * Deduplicates concurrent loads; shared across import, /api/data, and pivot.
 */
const DEFAULT_TTL_MS = Number(process.env.MASTER_CACHE_TTL_MS) || 5 * 60 * 1000;

const store = new Map();

/**
 * @template T
 * @param {string} key
 * @param {() => Promise<T>} loader
 * @param {number} [ttlMs]
 * @returns {Promise<T>}
 */
export async function getOrLoadMaster(key, loader, ttlMs = DEFAULT_TTL_MS) {
  const now = Date.now();
  const existing = store.get(key);
  if (existing?.value != null && now - existing.loadedAt < ttlMs) {
    return existing.value;
  }
  if (existing?.promise) return existing.promise;

  const promise = (async () => {
    const value = await loader();
    store.set(key, { value, loadedAt: Date.now() });
    return value;
  })().finally(() => {
    const cur = store.get(key);
    if (cur?.promise) {
      store.set(key, { value: cur.value, loadedAt: cur.loadedAt });
    }
  });

  store.set(key, {
    promise,
    value: existing?.value,
    loadedAt: existing?.loadedAt ?? 0,
  });
  return promise;
}

/** Tests or admin: optional explicit invalidation */
export function invalidateMasterCache(key) {
  if (key) store.delete(key);
  else store.clear();
}

/** Drop cached entries whose key starts with `prefix` (e.g. sales_data_count after import). */
export function invalidateMasterCachePrefix(prefix) {
  if (!prefix) return;
  for (const k of [...store.keys()]) {
    if (k.startsWith(prefix)) store.delete(k);
  }
}
