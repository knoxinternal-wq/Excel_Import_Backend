import jwt from 'jsonwebtoken';

const AUTH_TOKEN_ISSUER = 'excel-import-backend';
const AUTH_TOKEN_AUDIENCE = 'excel-import-ui';

function getAuthSessionSecret() {
  return String(
    process.env.AUTH_SESSION_SECRET
    || process.env.SUPABASE_SERVICE_ROLE_KEY
    || process.env.SERVICE_ROLE_KEY
    || '',
  ).trim();
}

function getAuthTokenTtl() {
  return String(process.env.AUTH_TOKEN_TTL || '12h').trim();
}

export function signAuthSessionToken(user) {
  const secret = getAuthSessionSecret();
  if (!secret) {
    throw new Error('Missing AUTH_SESSION_SECRET (or SUPABASE_SERVICE_ROLE_KEY) for auth token signing.');
  }
  return jwt.sign(
    {
      sub: String(user.id),
      email: String(user.email || '').toLowerCase(),
      is_admin: Boolean(user.is_admin),
    },
    secret,
    {
      expiresIn: getAuthTokenTtl(),
      issuer: AUTH_TOKEN_ISSUER,
      audience: AUTH_TOKEN_AUDIENCE,
    },
  );
}

export function verifyAuthSessionToken(token) {
  const secret = getAuthSessionSecret();
  if (!secret) {
    throw new Error('Missing AUTH_SESSION_SECRET (or SUPABASE_SERVICE_ROLE_KEY) for auth token verification.');
  }
  return jwt.verify(String(token || ''), secret, {
    issuer: AUTH_TOKEN_ISSUER,
    audience: AUTH_TOKEN_AUDIENCE,
  });
}

