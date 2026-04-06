/**
 * Import party_grouping_master from CSV.
 * CSV format: to_party_name, party_grouped, party_name_for_count
 * Run: node scripts/import-party-grouping.js path/to/file.csv
 *
 * Table has only 3 columns: TO PARTY NAME, PARTY GROUPED, PARTY NAME FOR COUNT
 */
import 'dotenv/config';
import { supabase } from '../models/supabase.js';
import fs from 'fs';
import path from 'path';

const csvPath = process.argv[2] || path.join(process.cwd(), 'scripts', 'party-grouping-template.csv');

function parseCSV(content) {
  const lines = content.split(/\r?\n/).filter((l) => l.trim());
  if (lines.length < 2) return [];
  const header = lines[0].split(',').map((h) => h.trim().toLowerCase());
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = lines[i].split(',').map((v) => v.trim().replace(/^"|"$/g, ''));
    if (vals[0]) {
      const getIdx = (names) => {
        for (const n of names) {
          const i = header.findIndex((h) => h.includes(n));
          if (i >= 0) return i;
        }
        return -1;
      };
      const iTo = getIdx(['to_party', 'party']) >= 0 ? getIdx(['to_party', 'party']) : 0;
      const iGrp = getIdx(['party_grouped', 'grouped']) >= 0 ? getIdx(['party_grouped', 'grouped']) : 1;
      const iCnt = getIdx(['party_name_for_count', 'for_count']) >= 0 ? getIdx(['party_name_for_count', 'for_count']) : 2;
      rows.push({
        to_party_name: vals[iTo] || vals[0],
        party_grouped: vals[iGrp] ?? vals[0],
        party_name_for_count: vals[iCnt] ?? vals[0],
      });
    }
  }
  return rows;
}

async function main() {
  if (!fs.existsSync(csvPath)) {
    console.error('File not found:', csvPath);
    console.log('Usage: node scripts/import-party-grouping.js <path-to-csv>');
    process.exit(1);
  }
  const content = fs.readFileSync(csvPath, 'utf-8');
  const rows = parseCSV(content);
  console.log(`Importing ${rows.length} rows from ${path.basename(csvPath)}`);
  let ok = 0, err = 0;
  for (const row of rows) {
    await supabase.from('party_grouping_master').delete().eq('to_party_name', row.to_party_name);
    const { error } = await supabase.from('party_grouping_master').insert({
      to_party_name: row.to_party_name,
      party_grouped: row.party_grouped || row.to_party_name,
      party_name_for_count: row.party_name_for_count || row.to_party_name,
    });
    if (error) {
      console.warn('Skip:', row.to_party_name, error.message);
      err++;
    } else ok++;
  }
  console.log(`Done: ${ok} ok, ${err} skipped`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
