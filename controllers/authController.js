import { findUserByEmail } from '../queries/authQueries.js';

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
    if (!user || user.password !== password) {
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    return res.json({
      token: makeSessionToken(user.email),
      user: {
        id: user.id,
        email: user.email,
        name: user.full_name || user.email,
      },
    });
  } catch (err) {
    return res.status(500).json({ error: err?.message || 'Login failed' });
  }
}

