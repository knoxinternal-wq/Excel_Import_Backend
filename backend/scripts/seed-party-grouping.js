/**
 * Seed party_grouping_master from distinct to_party_name in sales_data.
 * Run: cd backend && node scripts/seed-party-grouping.js
 *
 * For "NAME -LOCATION" format (e.g. "LINEN FABRICS -GANGAVATHI"):
 *   party_grouped = "NAME" (part before " -")
 *   party_name_for_count = full name
 *
 * Otherwise: party_grouped = party_name_for_count = to_party_name
 */
import 'dotenv/config';
import { supabase, getSupabaseAdminOrThrow } from '../models/supabase.js';

function derivePartyGrouped(fullName) {
  const s = String(fullName || '').trim();
  if (!s) return null;
  // "LINEN FABRICS -GANGAVATHI" -> "LINEN FABRICS"
  const match = s.match(/^(.+?)\s+-\s*[\w\s]+$/);
  return match ? match[1].trim() : s;
}

async function seed() {
  console.log('=== Seeding party_grouping_master from sales_data ===\n');

  // 1. Fetch distinct to_party_name from sales_data
  console.log('1. Fetching distinct to_party_name from sales_data...');
  const { data: salesRows, error: salesError } = await supabase
    .from('sales_data')
    .select('to_party_name');
  if (salesError) {
    console.error('   FAILED:', salesError.message);
    return;
  }
  const distinctNames = [...new Set((salesRows || []).map((r) => r.to_party_name).filter(Boolean))];
  console.log(`   Found ${distinctNames.length} distinct TO PARTY names\n`);

  if (distinctNames.length === 0) {
    console.warn('   No to_party_name values in sales_data. Import Excel data first.');
    return;
  }

  // 2. Fetch existing to_party_name (case-insensitive check)
  console.log('2. Checking existing party_grouping_master...');
  const { data: existingRows, error: existingError } = await supabase
    .from('party_grouping_master')
    .select('to_party_name');
  const existingSet = new Set(
    (existingRows || []).map((r) => (r.to_party_name || '').trim().toLowerCase())
  );
  console.log(`   Existing: ${existingSet.size} rows\n`);

  // 3. Build rows to insert (skip duplicates)
  const toInsert = [];
  for (const name of distinctNames) {
    const trimmed = String(name).trim();
    if (!trimmed) continue;
    const key = trimmed.toLowerCase();
    if (existingSet.has(key)) continue;
    existingSet.add(key);
    toInsert.push({
      to_party_name: trimmed,
      party_grouped: derivePartyGrouped(trimmed),
      party_name_for_count: trimmed,
    });
  }

  if (toInsert.length === 0) {
    console.log('   All party names already exist in party_grouping_master. Nothing to insert.');
    return;
  }

  console.log(`3. Inserting ${toInsert.length} new rows...`);
  const admin = getSupabaseAdminOrThrow();
  const BATCH = 100;
  let inserted = 0;
  for (let i = 0; i < toInsert.length; i += BATCH) {
    const batch = toInsert.slice(i, i + BATCH);
    const { error: insertError } = await admin.from('party_grouping_master').insert(batch);
    if (insertError) {
      console.error('   FAILED:', insertError.message);
      return;
    }
    inserted += batch.length;
    console.log(`   Inserted ${inserted}/${toInsert.length}`);
  }

  console.log('\n=== Done: party_grouping_master seeded ===');
  console.log('Refresh the app to see PARTY GROUPED and PARTY NAME FOR COUNT with correct values.');
}

seed().catch((err) => {
  console.error('Seed failed:', err);
  process.exit(1);
});
