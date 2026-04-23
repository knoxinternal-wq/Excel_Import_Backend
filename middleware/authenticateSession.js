import { findUserByEmail } from '../queries/authQueries.js';
import { verifyAuthSessionToken } from '../utils/authSessionToken.js';

function parseLegacySessionToken(token) {
  try {
    const raw = Buffer.from(String(token || ''), 'base64url').toString('utf8');
    const i = raw.indexOf(':');
    if (i <= 0) return null;
    const email = raw.slice(0, i).trim().toLowerCase();
    return email ? { email } : null;
  } catch {
    return null;
  }
}

function resolveEmailFromToken(token) {
  try {
    const parsed = verifyAuthSessionToken(token);
    const email = String(parsed?.email || '').trim().toLowerCase();
    if (email) return email;
  } catch {
    /* try legacy fallback below */
  }
  const allowLegacy = String(process.env.AUTH_ALLOW_LEGACY_TOKEN || '1').trim() !== '0';
  if (!allowLegacy) return '';
  return String(parseLegacySessionToken(token)?.email || '').trim().toLowerCase();
}

/**
 * Requires `Authorization: Bearer <token>` from login; attaches req.user for downstream handlers.
 */
export async function requireAuthSession(req, res, next) {
  try {
    const h = req.headers.authorization;
    if (!h || !String(h).startsWith('Bearer ')) {
      return res.status(401).json({ error: 'Unauthenticated' });
    }
    const token = String(h).slice(7).trim();
    const email = resolveEmailFromToken(token);
    if (!email) {
      return res.status(401).json({ error: 'Unauthenticated' });
    }
    const user = await findUserByEmail(email);
    if (!user || user.is_active === false) {
      return res.status(401).json({ error: 'Unauthenticated' });
    }
    req.user = {
      id: user.id,
      email: user.email,
      is_admin: Boolean(user.is_admin),
    };
    return next();
  } catch {
    return res.status(401).json({ error: 'Unauthenticated' });
  }
}
