import { createClient } from '@supabase/supabase-js';

/** Same project as backend: public URL + anon key (do not put DATABASE_URL in the browser). */
const supabaseUrl = String(import.meta.env.VITE_SUPABASE_URL || '').trim();
const supabaseAnonKey = String(
  import.meta.env.VITE_ANON_KEY || import.meta.env.VITE_SUPABASE_PUBLISHABLE_KEY || '',
).trim();

if (!supabaseUrl || !supabaseAnonKey) {
  console.warn(
    'Supabase env vars are missing. Set VITE_SUPABASE_URL and VITE_ANON_KEY (same values as backend SUPABASE_URL / DATABASE_URL host and ANON_KEY).',
  );
}

let client = null;

export function getSupabaseClient() {
  if (!client) {
    client = createClient(supabaseUrl, supabaseAnonKey);
  }
  return client;
}

