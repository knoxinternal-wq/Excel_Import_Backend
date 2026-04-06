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
