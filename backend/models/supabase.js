import { createClient } from '@supabase/supabase-js';
import { getSupabaseHttpUrl } from '../config/database.js';

/** Supabase REST: ANON_KEY + project URL (SUPABASE_URL or derived from DATABASE_URL). */
const supabaseUrl = getSupabaseHttpUrl();
const supabaseAnonKey = String(process.env.ANON_KEY || '').trim();

if (!supabaseAnonKey) {
  throw new Error(
    'Missing ANON_KEY in environment (Supabase Dashboard → Project Settings → API → anon public key).',
  );
}
if (!supabaseUrl) {
  throw new Error(
    'Missing Supabase project URL: set SUPABASE_URL (https://<ref>.supabase.co) or use DATABASE_URL pointing at Supabase (host db.<ref>.supabase.co, or pooler with user postgres.<ref>) so the URL can be derived.',
  );
}

export const supabase = createClient(supabaseUrl, supabaseAnonKey);

const serviceKey = String(process.env.SUPABASE_SERVICE_ROLE_KEY || '').trim();

/** Service role client for INSERT/UPDATE/DELETE (bypasses RLS). Omit key to disable writes via REST. */
export const supabaseAdmin =
  serviceKey ? createClient(supabaseUrl, serviceKey, { auth: { persistSession: false } }) : null;

export function getSupabaseAdminOrThrow() {
  if (!supabaseAdmin) {
    throw new Error('SUPABASE_SERVICE_ROLE_KEY is required for this operation.');
  }
  return supabaseAdmin;
}
