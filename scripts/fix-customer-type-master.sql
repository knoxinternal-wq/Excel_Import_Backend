-- Fix customer_type_master: add party_name column if it doesn't exist
-- Run this in Supabase SQL Editor if you get "column party_name does not exist"

-- Add column (idempotent - no error if column exists)
ALTER TABLE customer_type_master ADD COLUMN IF NOT EXISTS party_name VARCHAR(500);

-- Add type column if missing (some older schemas may have used different names)
ALTER TABLE customer_type_master ADD COLUMN IF NOT EXISTS type VARCHAR(255);

-- Drop and recreate the unique index
DROP INDEX IF EXISTS idx_customer_type_master_party_name_lower;
CREATE UNIQUE INDEX idx_customer_type_master_party_name_lower
  ON customer_type_master (LOWER(TRIM(party_name)));
