import bcrypt from 'bcrypt';
import { findUserByEmail } from '../queries/authQueries.js';
import { getPgPool } from '../config/database.js';

function makeSessionToken(email) {
  const raw = `${email}:${Date.now()}`;
  return Buffer.from(raw, 'utf8').toString('base64url');
}

export async function login(req, res) {
  try {
    const email = String(req.body?.email || '').trim().toLowerCase();
    const password = String(req.body?.password || '');

    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password are required' });
    }

    const user = await findUserByEmail(email);
    if (!user?.password) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const storedPassword = String(user.password || '');
    let valid = false;
    let shouldUpgradeHash = false;
    try {
      // bcrypt hashes start with $2a$, $2b$, or $2y$.
      const looksBcryptHash = /^\$2[aby]\$/.test(storedPassword);
      if (looksBcryptHash) {
        valid = await bcrypt.compare(password, storedPassword);
      } else {
        // Backward compatibility for legacy plaintext rows.
        valid = password === storedPassword;
        shouldUpgradeHash = valid;
      }
    } catch {
      valid = false;
    }
    if (!valid) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const pool = getPgPool();
    if (pool && user.id != null) {
      try {
        if (shouldUpgradeHash) {
          const upgraded = await bcrypt.hash(password, 12);
          await pool.query('UPDATE app_users SET password = $1 WHERE id = $2', [upgraded, user.id]);
        }
        await pool.query('UPDATE app_users SET last_login_at = NOW() WHERE id = $1', [user.id]);
      } catch {
        /* ignore */
      }
    }

    return res.json({
      token: makeSessionToken(user.email),
      user: {
        id: user.id,
        email: user.email,
        name: user.full_name || user.email,
        is_admin: Boolean(user.is_admin),
      },
    });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'Login failed' });
  }
}
