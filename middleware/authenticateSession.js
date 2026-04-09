import { findUserByEmail } from '../queries/authQueries.js';

function parseSessionToken(token) {
  try {
    const raw = Buffer.from(String(token), 'base64url').toString('utf8');
    const i = raw.indexOf(':');
    if (i < 1) return null;
    return { email: raw.slice(0, i).trim().toLowerCase() };
  } catch {
    return null;
  }
}

/**
 * Requires `Authorization: Bearer <token>` from login; attaches req.user for downstream handlers.
 */
export async function requireAuthSession(req, res, next) {
  const h = req.headers.authorization;
  if (!h || !String(h).startsWith('Bearer ')) {
    return res.status(401).json({ error: 'Unauthenticated' });
  }
  const token = String(h).slice(7).trim();
  const parsed = parseSessionToken(token);
  if (!parsed?.email) {
    return res.status(401).json({ error: 'Unauthenticated' });
  }
  const user = await findUserByEmail(parsed.email);
  if (!user || user.is_active === false) {
    return res.status(401).json({ error: 'Unauthenticated' });
  }
  req.user = {
    id: user.id,
    email: user.email,
    is_admin: Boolean(user.is_admin),
  };
  return next();
}
