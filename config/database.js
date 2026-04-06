/**
 * Single source for PostgreSQL connectivity: `DATABASE_URL` only.
 * SSL: enabled with `rejectUnauthorized: false` (Supabase direct + pooler :6543).
 * Set `DATABASE_SSL_STRICT=1` to use URL defaults without relaxing TLS.
 */
import pg from 'pg';
import pgConnString from 'pg-connection-string';
import { logError } from '../utils/logger.js';

/** @type {import('pg').Pool | null} */
let pool = null;

export function getDatabaseUrl() {
  return String(process.env.DATABASE_URL || '').trim();
}

export function assertDatabaseUrl() {
  const url = getDatabaseUrl();
  if (!url) {
    throw new Error(
      'Set DATABASE_URL in backend/.env (PostgreSQL URI from Supabase → Project Settings → Database).',
    );
  }
  return url;
}

/**
 * HTTPS base URL for `@supabase/supabase-js` (PostgREST). Used with ANON_KEY for REST reads/writes.
 * Order: `SUPABASE_URL` if set, else derive from `DATABASE_URL` when it is a Supabase Postgres URI:
 * - host `db.<project_ref>.supabase.co`, or
 * - `*.pooler.supabase.com` with username `postgres.<project_ref>`.
 * Non-Supabase DATABASE_URL → return '' (then set SUPABASE_URL explicitly).
 * @returns {string}
 */
export function getSupabaseHttpUrl() {
  const explicit = String(process.env.SUPABASE_URL || '').trim();
  if (explicit) return explicit.replace(/\/+$/, '');

  const dbUrl = getDatabaseUrl();
  if (!dbUrl) return '';

  try {
    const normalized = dbUrl.replace(/^postgres:\/\//i, 'postgresql://');
    const u = new URL(normalized);
    const host = u.hostname || '';

    const direct = host.match(/^db\.([^.]+)\.supabase\.co$/i);
    if (direct) {
      return `https://${direct[1]}.supabase.co`;
    }

    if (/pooler\.supabase\.com$/i.test(host)) {
      const rawUser = u.username ? decodeURIComponent(u.username) : '';
      const poolerUser = rawUser.match(/^postgres\.([^.]+)$/i);
      if (poolerUser) {
        return `https://${poolerUser[1]}.supabase.co`;
      }
    }
  } catch {
    /* ignore parse errors */
  }
  return '';
}

function keepAliveInitialDelayMillis() {
  const n = Number(process.env.IMPORT_PG_KEEPALIVE_MS);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : 10_000;
}

/**
 * Client/pool config from URI. Avoids pg overwriting custom `ssl` when mixing `connectionString` + `ssl`.
 * @param {string} connectionString
 * @returns {import('pg').ClientConfig}
 */
export function buildPgConfigFromUrl(connectionString) {
  const strict = String(process.env.DATABASE_SSL_STRICT || '').trim() === '1';
  const keepAliveMs = keepAliveInitialDelayMillis();
  const lower = String(connectionString).toLowerCase();

  if (lower.includes('sslmode=disable')) {
    const cfg = pgConnString.parseIntoClientConfig(connectionString);
    cfg.keepAlive = true;
    cfg.keepAliveInitialDelayMillis = keepAliveMs;
    return cfg;
  }

  const cfg = pgConnString.parseIntoClientConfig(connectionString);
  cfg.keepAlive = true;
  cfg.keepAliveInitialDelayMillis = keepAliveMs;

  if (strict) {
    return cfg;
  }

  const prev = cfg.ssl;
  cfg.ssl =
    typeof prev === 'object' && prev !== null
      ? { ...prev, rejectUnauthorized: false }
      : { rejectUnauthorized: false };
  return cfg;
}

function poolMaxConnections() {
  const raw = String(process.env.PG_POOL_MAX || '').trim();
  const n = raw ? Number(raw) : NaN;
  if (Number.isFinite(n) && n >= 1) return Math.min(50, Math.floor(n));
  return 15;
}

function poolConnectionTimeoutMs() {
  const n = Number(process.env.PG_CONNECTION_TIMEOUT_MS);
  return Number.isFinite(n) && n >= 1000 ? Math.floor(n) : 30_000;
}

/**
 * @param {string} connectionString
 * @returns {import('pg').PoolConfig}
 */
export function buildPoolConfigFromUrl(connectionString) {
  const base = buildPgConfigFromUrl(connectionString);
  return {
    ...base,
    max: poolMaxConnections(),
    idleTimeoutMillis: Number(process.env.PG_POOL_IDLE_MS) > 0
      ? Math.floor(Number(process.env.PG_POOL_IDLE_MS))
      : 30_000,
    connectionTimeoutMillis: poolConnectionTimeoutMs(),
    allowExitOnIdle: false,
  };
}

/**
 * Shared pool for API-adjacent work, pivot SQL, and import helpers that `connect()` a dedicated client.
 * Returns null if `DATABASE_URL` is unset (callers that need optional DB skip).
 * @returns {import('pg').Pool | null}
 */
export function getPgPool() {
  const url = getDatabaseUrl();
  if (!url) return null;
  if (!pool) {
    pool = new pg.Pool(buildPoolConfigFromUrl(url));
    pool.on('error', (err) => {
      logError('db', 'PostgreSQL pool error', { message: err?.message, code: err?.code });
    });
  }
  return pool;
}

/**
 * @returns {Promise<void>}
 */
export async function endPgPool() {
  if (!pool) return;
  const p = pool;
  pool = null;
  await p.end();
}
