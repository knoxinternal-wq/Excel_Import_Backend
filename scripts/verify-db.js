/**
 * Database Verification Script
 * Run: node scripts/verify-db.js
 * Verifies that Excel import data can be written to and read from the database.
 */
import 'dotenv/config';
import { supabase } from '../models/supabase.js';

async function verify() {
  console.log('=== Database Verification ===\n');

  // 1. Test connection - fetch count
  console.log('1. Testing Supabase connection...');
  const { count, error: countError } = await supabase
    .from('sales_data')
    .select('*', { count: 'exact', head: true });
  if (countError) {
    console.error('   FAILED:', countError.message);
    return;
  }
  console.log(`   OK - sales_data table accessible (current rows: ${count ?? 0})\n`);

  // 1b. Check party_grouping_master (required for party_grouped / party_name_for_count mapping)
  console.log('1b. Checking party_grouping_master...');
  const { data: pgRows, error: pgError } = await supabase
    .from('party_grouping_master')
    .select('to_party_name, party_grouped, party_name_for_count');
  if (pgError) {
    console.error('   FAILED:', pgError.message);
    console.error('   → Run: ALTER TABLE party_grouping_master ENABLE ROW LEVEL SECURITY;');
    console.error('   → Then: CREATE POLICY "Allow anon full access to party_grouping_master" ON party_grouping_master FOR ALL TO anon USING (true) WITH CHECK (true);');
    return;
  }
  const pgCount = pgRows?.length ?? 0;
  console.log(`   OK - party_grouping_master accessible (${pgCount} mappings)`);
  if (pgCount === 0) {
    console.warn('   WARNING: party_grouping_master is empty. Populate it for party_grouped/party_name_for_count mapping.');
    console.warn('   Without it, import will use TO PARTY NAME as fallback for both columns.');
    console.warn('   Example: INSERT INTO party_grouping_master (to_party_name, party_grouped, party_name_for_count) VALUES (\'ABC TRADERS\', \'ABC GROUP\', \'ABC TRADERS\');');
  }
  console.log('');

  // 1c. party_master_app (district + pin_code from TO PARTY NAME)
  console.log('1c. Checking party_master_app (district / PIN CODE master)...');
  const { count: partyCount, error: partyErr } = await supabase
    .from('party_master_app')
    .select('*', { count: 'exact', head: true });
  if (partyErr) {
    console.error('   FAILED (anon REST):', partyErr.message);
    console.error('   → With DATABASE_URL set, the app still loads this table via Postgres; otherwise add RLS SELECT for anon or run: npm run db:diagnose-party-master');
  } else {
    console.log(`   anon REST row count: ${partyCount ?? 0}`);
    if ((partyCount ?? 0) === 0) {
      console.warn('   If the table has data in SQL Editor but count is 0 here, RLS is blocking anon.');
      console.warn('   Run: npm run db:diagnose-party-master');
    }
  }
  console.log('');

  // 2. Insert a test row (simulating Excel import data)
  console.log('2. Inserting test row (simulating import)...');
  const testRow = {
    branch: 'Test Branch',
    business_type: 'RETAILER',
    brand: 'Test Brand',
    agent_name: 'Test Agent',
    to_party_name: 'Test Customer',
    bill_no: 'VERIFY-001',
    bill_date: '2026-02-28', // YYYY-MM-DD format
    item_no: 'Test Item',
    shade_name: '-',
    rate_unit: 100,
    sl_qty: 5,
    gross_amount: 500,
    amount_before_tax: 450,
    net_amount: 425,
    sale_order_no: 'SO-VERIFY',
    sale_order_date: '2026-02-25',
    goods_type: 'Fresh',
  };

  const { data: inserted, error: insertError } = await supabase
    .from('sales_data')
    .insert(testRow)
    .select('id, bill_no, bill_date, item_no, net_amount')
    .single();

  if (insertError) {
    console.error('   FAILED:', insertError.message);
    console.error('   Details:', insertError);
    return;
  }
  console.log('   OK - Row inserted:', inserted);

  // 3. Read it back
  console.log('\n3. Reading back inserted row...');
  const { data: fetched, error: fetchError } = await supabase
    .from('sales_data')
    .select('id, branch, bill_no, bill_date, item_no, net_amount, created_at')
    .eq('id', inserted.id)
    .single();

  if (fetchError) {
    console.error('   FAILED:', fetchError.message);
    return;
  }
  console.log('   OK - Data retrieved:', JSON.stringify(fetched, null, 2));

  // 4. Clean up test row
  console.log('\n4. Deleting test row...');
  const { error: deleteError } = await supabase
    .from('sales_data')
    .delete()
    .eq('id', inserted.id);
  if (deleteError) {
    console.warn('   WARNING: Could not delete test row:', deleteError.message);
  } else {
    console.log('   OK - Test row removed');
  }

  console.log('\n=== Verification complete: Database is working correctly ===');
}

verify().catch((err) => {
  console.error('Verification failed:', err);
  process.exit(1);
});
