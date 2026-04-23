import { findUserByEmail } from '../queries/authQueries.js';
import { verifyAuthSessionToken } from '../utils/authSessionToken.js';

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
    const parsed = verifyAuthSessionToken(token);
    const email = String(parsed?.email || '').trim().toLowerCase();
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
