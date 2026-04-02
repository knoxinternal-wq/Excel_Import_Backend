-- Seed party_grouping_master from sales_data
-- Run this in Supabase SQL Editor.
--
-- Table has only 3 columns: to_party_name (TO PARTY NAME), party_grouped, party_name_for_count
-- Match: sales_data.to_party_name = party_grouping_master.to_party_name

CREATE TABLE IF NOT EXISTS party_grouping_master (
  id SERIAL PRIMARY KEY,
  to_party_name VARCHAR(500) NOT NULL,
  party_grouped VARCHAR(500),
  party_name_for_count VARCHAR(500),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Run fix-party-grouping-master-3-cols.sql first if migrating from old schema with party_name.

INSERT INTO party_grouping_master (to_party_name, party_grouped, party_name_for_count)
SELECT DISTINCT
  TRIM(s.to_party_name),
  TRIM(s.to_party_name),
  TRIM(s.to_party_name)
FROM sales_data s
WHERE s.to_party_name IS NOT NULL AND TRIM(s.to_party_name) <> ''
  AND NOT EXISTS (
    SELECT 1 FROM party_grouping_master p
    WHERE LOWER(TRIM(p.to_party_name)) = LOWER(TRIM(s.to_party_name))
  );
