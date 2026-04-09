import fs from 'node:fs';
import path from 'node:path';

function loadEnvFromFile(envPath) {
  if (!fs.existsSync(envPath)) return;
  const raw = fs.readFileSync(envPath, 'utf8');
  for (const line of raw.split(/\r?\n/)) {
    const s = line.trim();
    if (!s || s.startsWith('#')) continue;
    const idx = s.indexOf('=');
    if (idx <= 0) continue;
    const key = s.slice(0, idx).trim();
    const val = s.slice(idx + 1).trim();
    if (!key || !val) continue;
    if (process.env[key] == null) process.env[key] = val;
  }
}

async function main() {
  const envPath = path.resolve(process.cwd(), 'backend', '.env');
  loadEnvFromFile(envPath);

  const { supabase } = await import('../models/supabase.js');
  const masterNull = await supabase
    .from('district_master_data')
    .select('account_name,district', { count: 'exact' })
    .or('district.is.null,district.eq.');

  if (masterNull.error) throw new Error(masterNull.error.message);

  const masterNullSample = await supabase
    .from('district_master_data')
    .select('account_name,district')
    .or('district.is.null,district.eq.')
    .limit(10);

  const salesNullSample = await supabase
    .from('sales_data')
    .select('to_party_name,state,city,district')
    .is('district', null)
    .limit(10);

  console.log('district_master_data rows with district null/empty:', masterNull.count ?? 0);
  console.log('sample district_master_data null/empty district:', masterNullSample.data ?? []);
  console.log('sample sales_data district null:', salesNullSample.data ?? []);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});

