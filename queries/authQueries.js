import { getPgPool } from '../config/database.js';
import { logError } from '../utils/logger.js';

const USER_CACHE_TTL_MS = Number(process.env.AUTH_USER_CACHE_TTL_MS) || 10 * 60 * 1000;
const AUTH_LOOKUP_TIMEOUT_MS = Number(process.env.AUTH_LOOKUP_TIMEOUT_MS) || 5_000;
const userCache = new Map();
const userInflight = new Map();

function normalizeEmail(email) {
  return String(email || '').trim().toLowerCase();
}

function withTimeout(promise, timeoutMs) {
  const ms = Number.isFinite(timeoutMs) && timeoutMs > 0 ? Math.floor(timeoutMs) : 5_000;
  return Promise.race([
    promise,
    new Promise((resolve) => {
      setTimeout(() => resolve(null), ms);
    }),
  ]);
}

async function fetchUserFromPg(normalized) {
  const pool = getPgPool();
  if (!pool) return null;

  try {
    const { rows } = await pool.query(
      `SELECT id, email, password, full_name, is_active, is_admin, last_login_at
       FROM app_users
       WHERE email = $1 AND is_active = true
       LIMIT 1`,
      [normalized],
    );
    return rows?.[0] ?? null;
  } catch (e) {
    logError('auth', 'fetchUserFromPg failed', {
      message: e?.message,
      code: e?.code,
    });
    return null;
  }
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
    const pgUser = await withTimeout(fetchUserFromPg(normalized), AUTH_LOOKUP_TIMEOUT_MS);
    return pgUser;
  })();

  userInflight.set(normalized, p);
  try {
    const user = await p;
    userCache.set(normalized, { ts: now, user });
    return user;
  } finally {
    userInflight.delete(normalized);
  }
}
