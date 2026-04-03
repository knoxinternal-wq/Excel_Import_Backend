import { supabase } from '../models/supabase.js';

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

export async function findUserByEmail(email) {
  const normalized = normalizeEmail(email);
  if (!normalized) return null;

  const [, data] = await Promise.all([
    seedDefaultUsersIfPossible(),
    fetchUserFromSupabase(normalized),
  ]);

  if (data) return data;

  const fallback = DEFAULT_USERS.find((u) => normalizeEmail(u.email) === normalized && u.is_active);
  if (!fallback) return null;

  return {
    id: `local-${normalized}`,
    email: fallback.email,
    password: fallback.password,
    full_name: fallback.full_name,
    is_active: fallback.is_active,
  };
}

