import 'dotenv/config';
import { refreshPivotMVs } from '../services/pivotMvRefresh.js';
import { endPgPool } from '../config/database.js';

async function main() {
  try {
    await refreshPivotMVs();
    console.log('Pivot MV refresh complete for all materialized views.');
  } catch (e) {
    console.error(e?.message || e);
    process.exit(1);
  } finally {
    await endPgPool().catch(() => {});
  }
}

main();
