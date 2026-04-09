import 'dotenv/config';
import bcrypt from 'bcrypt';
import { getPgPool, endPgPool } from '../config/database.js';

const users = [
  { email: 'vishal@rishabworld.com', name: 'Vishal', pw: process.env.VISHAL_INIT_PW },
  { email: 'greshma@rishabworld.com', name: 'Greshma', pw: process.env.GRESHMA_INIT_PW },
];

async function main() {
  const pool = getPgPool();
  if (!pool) {
    console.error('DATABASE_URL is required');
    process.exit(1);
  }
  for (const u of users) {
    if (!u.pw) {
      console.error('Missing env var for', u.email);
      process.exit(1);
    }
    const hash = await bcrypt.hash(u.pw, 12);
    await pool.query(
      `INSERT INTO app_users (email, password, full_name, is_admin)
       VALUES ($1,$2,$3,$4) ON CONFLICT (email) DO NOTHING`,
      [u.email, hash, u.name, false],
    );
    console.log('Seeded:', u.email);
  }
  await endPgPool();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
