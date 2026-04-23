import { getSupabaseHttpUrl } from './database.js';

function has(v) {
  return String(v || '').trim().length > 0;
}

function isProduction() {
  return String(process.env.NODE_ENV || '').trim().toLowerCase() === 'production';
}

function looksLikeWeakSeedPassword(v) {
  const s = String(v || '').trim();
  if (!s) return false;
  return /@123$/i.test(s) || s.length < 10;
}

export function validateEnvironment() {
  const errors = [];
  const warnings = [];

  if (!has(process.env.DATABASE_URL)) {
    errors.push('Missing DATABASE_URL.');
  }

  if (!has(process.env.ANON_KEY)) {
    errors.push('Missing ANON_KEY.');
  }

  if (!has(process.env.SUPABASE_URL) && !getSupabaseHttpUrl()) {
    errors.push('Missing SUPABASE_URL (or a Supabase DATABASE_URL from which URL can be derived).');
  }

  if (!has(process.env.SUPABASE_SERVICE_ROLE_KEY) && has(process.env.SERVICE_ROLE_KEY)) {
    warnings.push('Using SERVICE_ROLE_KEY alias. Prefer SUPABASE_SERVICE_ROLE_KEY.');
  }

  if (!has(process.env.AUTH_SESSION_SECRET)) {
    const hasServiceRole =
      has(process.env.SUPABASE_SERVICE_ROLE_KEY) || has(process.env.SERVICE_ROLE_KEY);
    if (isProduction()) {
      errors.push('Missing AUTH_SESSION_SECRET in production.');
    } else if (!hasServiceRole) {
      warnings.push(
        'AUTH_SESSION_SECRET is not set and no service-role fallback key is available.',
      );
    }
  }

  if (looksLikeWeakSeedPassword(process.env.VISHAL_INIT_PW) || looksLikeWeakSeedPassword(process.env.GRESHMA_INIT_PW)) {
    warnings.push('Detected weak/default seeded passwords in env; rotate and avoid committing local secrets.');
  }

  return { errors, warnings };
}

