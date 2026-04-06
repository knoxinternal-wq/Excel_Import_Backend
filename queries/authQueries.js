import { supabase } from '../models/supabase.js';
import { getPgPool } from '../config/database.js';
import { logError } from '../utils/logger.js';

const DEFAULT_USERS = [
  {
    email: 'vishal@rishabworld.com',
    password: 'Vishal@123',
    full_name: 'Vishal',
    is_active: true,
  },
  {
    email: 'greshma@rishabworld.com',
    password: 'Greshma@123',
    full_name: 'Greshma',
    is_active: true,
  },
];

let seedAttempted = false;

const USER_CACHE_TTL_MS = Number(process.env.AUTH_USER_CACHE_TTL_MS) || 10 * 60 * 1000;
const userCache = new Map(); // normalizedEmail -> { ts, user }
const userInflight = new Map(); // normalizedEmail -> Promise<user|null>

function normalizeEmail(email) {
  return String(email || '').trim().toLowerCase();
}

async function seedDefaultUsersIfPossible() {
  if (seedAttempted) return;
  seedAttempted = true;

  try {
    // Best-effort seed. If table/policy does not exist yet, fallback users still work.
    await supabase
      .from('app_users')
      .upsert(DEFAULT_USERS, { onConflict: 'email' });
  } catch {
    // Ignore: fallback auth handles missing table.
  }
}

async function fetchUserFromSupabase(normalized) {
  try {
    const { data, error } = await supabase
      .from('app_users')
      .select('id, email, password, full_name, is_active')
      .eq('email', normalized)
      .eq('is_active', true)
      .maybeSingle();

    if (!error && data) return data;
  } catch {
    // Ignore and fallback to default users.
  }
  return null;
}

async function fetchUserFromPg(normalized) {
  const pool = getPgPool();
  if (!pool) return null;

  try {
    // Uses direct Postgres lookup for speed. On timeout / pool exhaustion / network issues, fall
    // back to Supabase REST + hardcoded users instead of failing login with HTTP 500.
    const { rows } = await pool.query(
      `SELECT id, email, password, full_name, is_active
       FROM app_users
       WHERE email = $1 AND is_active = true
       LIMIT 1`,
      [normalized],
    );
    return rows?.[0] ?? null;
  } catch (e) {
    logError('auth', 'fetchUserFromPg failed; falling back to REST', {
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
    // Do not block login on seed (PostgREST can be slow); first request still has hardcoded fallback.
    void seedDefaultUsersIfPossible();

    // Fast path: direct Postgres.
    const pgUser = await fetchUserFromPg(normalized);
    if (pgUser) return pgUser;

    // Compatibility path: Supabase REST.
    const sbUser = await fetchUserFromSupabase(normalized);
    if (sbUser) return sbUser;

    // Final fallback: hardcoded dev users (works even if app_users table/policies aren't ready).
    const fallback = DEFAULT_USERS.find((u) => normalizeEmail(u.email) === normalized && u.is_active);
    if (!fallback) return null;

    return {
      id: `local-${normalized}`,
      email: fallback.email,
      password: fallback.password,
      full_name: fallback.full_name,
      is_active: fallback.is_active,
    };
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

