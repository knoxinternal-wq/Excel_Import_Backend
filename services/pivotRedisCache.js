/**
 * Optional Redis cache for pivot payloads (JSON).
 * Set REDIS_URL or REDISCLOUD_URL. If unset or Redis errors, callers skip Redis.
 */
import { createClient } from 'redis';

const TTL_SEC = Math.min(
  7200,
  Math.max(300, Number(process.env.PIVOT_REDIS_TTL_SEC) || 3600),
);

/** Pivot filter DISTINCT lists — longer TTL (Excel-like repeat opens). */
const FILTER_VALUES_TTL_SEC = Math.min(
  7200,
  Math.max(120, Number(process.env.PIVOT_FILTER_REDIS_TTL_SEC) || 3600),
);

let client = null;
let connecting = null;

function redisUrl() {
  return process.env.REDIS_URL || process.env.REDISCLOUD_URL || '';
}

export function isPivotRedisConfigured() {
  return Boolean(redisUrl());
}

async function getClient() {
  if (!isPivotRedisConfigured()) return null;
  if (client?.isOpen) return client;
  if (connecting) return connecting;
  connecting = (async () => {
    const c = createClient({ url: redisUrl() });
    c.on('error', () => {});
    await c.connect();
    client = c;
    connecting = null;
    return c;
  })().catch(() => {
    connecting = null;
    client = null;
    return null;
  });
  return connecting;
}

export async function pivotRedisGet(cacheKey) {
  try {
    const c = await getClient();
    if (!c) return null;
    const raw = await c.get(`pivot:v2:${cacheKey}`);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export async function pivotRedisSet(cacheKey, payload) {
  try {
    const c = await getClient();
    if (!c) return;
    await c.set(`pivot:v2:${cacheKey}`, JSON.stringify(payload), { EX: TTL_SEC });
  } catch {
    /* ignore */
  }
}

export async function pivotFilterValuesRedisGet(redisKey) {
  try {
    const c = await getClient();
    if (!c) return null;
    const raw = await c.get(`pivot:fv:v1:${redisKey}`);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export async function pivotFilterValuesRedisSet(redisKey, payload) {
  try {
    const c = await getClient();
    if (!c) return;
    await c.set(`pivot:fv:v1:${redisKey}`, JSON.stringify(payload), { EX: FILTER_VALUES_TTL_SEC });
  } catch {
    /* ignore */
  }
}

export { TTL_SEC as PIVOT_REDIS_TTL_SEC, FILTER_VALUES_TTL_SEC as PIVOT_FILTER_REDIS_TTL_SEC };
