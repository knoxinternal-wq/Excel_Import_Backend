-- Fix party_grouping_master: only 3 columns (TO PARTY NAME, PARTY GROUPED, PARTY NAME FOR COUNT)
-- Run this in Supabase SQL Editor if you have existing table with party_name.
--
-- Columns: to_party_name, party_grouped, party_name_for_count
-- Match: sales_data.to_party_name = party_grouping_master.to_party_name

-- Add to_party_name if missing (for tables that only had party_name)
ALTER TABLE party_grouping_master ADD COLUMN IF NOT EXISTS to_party_name VARCHAR(500);

-- Backfill to_party_name from party_name for existing rows
UPDATE party_grouping_master SET to_party_name = party_name WHERE (to_party_name IS NULL OR to_party_name = '') AND party_name IS NOT NULL;

-- Drop party_name column (table should have only to_party_name, party_grouped, party_name_for_count)
ALTER TABLE party_grouping_master DROP COLUMN IF EXISTS party_name;

-- Index for fast lookup
CREATE INDEX IF NOT EXISTS idx_party_grouping_master_to_party_name_lower
  ON party_grouping_master (LOWER(TRIM(to_party_name)));
