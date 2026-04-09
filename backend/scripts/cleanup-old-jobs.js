import 'dotenv/config';
import { getPgPool, endPgPool } from '../config/database.js';

async function main() {
  const pool = getPgPool();
  if (!pool) {
    console.error('DATABASE_URL is required');
    process.exit(1);
  }
  const result = await pool.query(`
    DELETE FROM import_jobs
    WHERE created_at < NOW() - INTERVAL '90 days'
      AND status IN ('completed','failed','cancelled')
  `);
  console.log('Deleted', result.rowCount, 'old import jobs');
  await endPgPool();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
