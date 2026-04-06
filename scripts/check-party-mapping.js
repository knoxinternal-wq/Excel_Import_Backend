/**
 * Party Mapping Diagnostic
 * Run: cd backend && node scripts/check-party-mapping.js
 * Shows party_grouping_master vs sales_data to_party_name to debug mapping.
 */
import 'dotenv/config';
import { supabase } from '../models/supabase.js';
import { normalizePartyName } from '../utils/normalizeHeader.js';

async function main() {
  console.log('=== Party Mapping Diagnostic ===\n');

  const { data: masterRows, error: masterErr } = await supabase
    .from('party_grouping_master')
    .select('to_party_name, party_grouped, party_name_for_count');
  if (masterErr) {
    console.error('party_grouping_master ERROR:', masterErr.message);
    return;
  }
  const masterKeys = new Set((masterRows || []).map((r) => normalizePartyName(r.to_party_name)));
  console.log('party_grouping_master:', (masterRows || []).length, 'rows');
  if ((masterRows || []).length === 0) {
    console.log('\n*** party_grouping_master is EMPTY ***');
    console.log('Add rows like:');
    console.log("  INSERT INTO party_grouping_master (to_party_name, party_grouped, party_name_for_count) VALUES ('LINEN FABRICS -GANGAVATHI', 'LINEN FABRICS GROUP', 'LINEN FABRICS -GANGAVATHI');");
    return;
  }
  console.log('Sample master keys:', [...masterKeys].slice(0, 5));
  console.log('');

  const { data: salesRows } = await supabase
    .from('sales_data')
    .select('to_party_name')
    .not('to_party_name', 'is', null)
    .limit(500);
  const distinct = [...new Set((salesRows || []).map((r) => r.to_party_name).filter(Boolean))];
  let matched = 0;
  let unmatched = [];
  for (const name of distinct.slice(0, 20)) {
    const key = normalizePartyName(name);
    if (masterKeys.has(key)) matched++;
    else unmatched.push({ raw: name, key });
  }
  console.log('Sample to_party_name from sales_data (first 20):');
  for (const u of unmatched) {
    console.log('  UNMATCHED:', JSON.stringify(u.raw), '→ key:', JSON.stringify(u.key));
  }
  if (unmatched.length === 0 && distinct.length > 0) {
    console.log('  All sampled keys matched.');
  }
  if (unmatched.length > 0) {
    console.log('\nAdd to party_grouping_master (to_party_name = TO PARTY NAME):');
    unmatched.slice(0, 5).forEach((u) => {
      console.log(`  INSERT INTO party_grouping_master (to_party_name, party_grouped, party_name_for_count) VALUES ('${u.raw.replace(/'/g, "''")}', '<GROUP_NAME>', '${u.raw.replace(/'/g, "''")}');`);
    });
  }
}

main().catch(console.error);
