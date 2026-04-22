import { getPgPool } from '../config/database.js';
import { supabase, supabaseAdmin } from '../models/supabase.js';
import { logError } from '../utils/logger.js';

const USER_CACHE_TTL_MS = Number(process.env.AUTH_USER_CACHE_TTL_MS) || 10 * 60 * 1000;
const AUTH_LOOKUP_TIMEOUT_MS = Number(process.env.AUTH_LOOKUP_TIMEOUT_MS) || 5_000;
const userCache = new Map();
const userInflight = new Map();
const AUTH_TIMEOUT = Symbol('AUTH_TIMEOUT');

function normalizeEmail(email) {
  return String(email || '').trim().toLowerCase();
}

function withTimeout(promise, timeoutMs) {
  const ms = Number.isFinite(timeoutMs) && timeoutMs > 0 ? Math.floor(timeoutMs) : 5_000;
  return Promise.race([
    promise,
    new Promise((resolve) => {
      setTimeout(() => resolve(AUTH_TIMEOUT), ms);
    }),
  ]);
}

async function fetchUserFromPg(normalized) {
  const pool = getPgPool();
  if (!pool) return null;
  const { rows } = await pool.query(
    `SELECT id, email, password, full_name, is_active, is_admin, last_login_at
     FROM app_users
     WHERE email = $1 AND is_active = true
     LIMIT 1`,
    [normalized],
  );
  return rows?.[0] ?? null;
}

async function fetchUserFromSupabase(normalized) {
  const client = supabaseAdmin || supabase;
  const { data, error } = await client
    .from('app_users')
    .select('id, email, password, full_name, is_active, is_admin, last_login_at')
    .eq('email', normalized)
    .eq('is_active', true)
    .limit(1);
  if (error) throw new Error(error.message || 'Supabase auth lookup failed');
  return data?.[0] ?? null;
}

export async function findUserByEmail(email) {
  const normalized = normalizeEmail(email);
  if (!normalized) return null;

  const now = Date.now();
  const cached = userCache.get(normalized);
  if (cached && now - cached.ts < USER_CACHE_TTL_MS) {
    return cached.user;
  }

  if (userInflight.has(normalized)) {
    return userInflight.get(normalized);
  }

  const p = (async () => {
    try {
      const pgUser = await withTimeout(fetchUserFromPg(normalized), AUTH_LOOKUP_TIMEOUT_MS);
      if (pgUser !== AUTH_TIMEOUT) return pgUser;
      logError('auth', 'findUserByEmail timeout on pg lookup; trying supabase fallback', {
        email: normalized,
      });
    } catch (e) {
      logError('auth', 'findUserByEmail pg lookup failed; trying supabase fallback', {
        email: normalized,
        message: e?.message,
        code: e?.code,
      });
    }

    try {
      const sbUser = await withTimeout(fetchUserFromSupabase(normalized), AUTH_LOOKUP_TIMEOUT_MS);
      if (sbUser === AUTH_TIMEOUT) {
        logError('auth', 'findUserByEmail timeout on supabase fallback', { email: normalized });
        return null;
      }
      return sbUser;
    } catch (e) {
      logError('auth', 'findUserByEmail supabase fallback failed', {
        email: normalized,
        message: e?.message,
        code: e?.code,
      });
      return null;
    }
  })();

  userInflight.set(normalized, p);
  try {
    const user = await p;
    // Do not cache misses aggressively; this avoids stale false 401s after transient DB issues.
    if (!user) return null;
    userCache.set(normalized, { ts: now, user });
    return user;
  } finally {
    userInflight.delete(normalized);
  }
}
