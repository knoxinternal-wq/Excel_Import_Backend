-- Performance hardening migration (no business logic changes).
-- Safe to run multiple times.

BEGIN;

-- 1) Normalized generated columns to avoid UPPER(TRIM()) predicates in query paths.
ALTER TABLE sales_data
  ADD COLUMN IF NOT EXISTS norm_party text
  GENERATED ALWAYS AS (upper(trim(coalesce(to_party_name, '')))) STORED;

ALTER TABLE sales_data
  ADD COLUMN IF NOT EXISTS norm_brand text
  GENERATED ALWAYS AS (upper(trim(coalesce(brand, '')))) STORED;

ALTER TABLE sales_data
  ADD COLUMN IF NOT EXISTS norm_agent text
  GENERATED ALWAYS AS (upper(trim(coalesce(agent_name, '')))) STORED;

ALTER TABLE sales_data
  ADD COLUMN IF NOT EXISTS norm_state text
  GENERATED ALWAYS AS (upper(trim(coalesce(state, '')))) STORED;

-- 2) Indexes for normalized predicates and reporting dimensions.
CREATE INDEX IF NOT EXISTS idx_sales_norm_party ON sales_data (norm_party);
CREATE INDEX IF NOT EXISTS idx_sales_norm_brand ON sales_data (norm_brand);
CREATE INDEX IF NOT EXISTS idx_sales_norm_agent ON sales_data (norm_agent);
CREATE INDEX IF NOT EXISTS idx_sales_norm_state ON sales_data (norm_state);
CREATE INDEX IF NOT EXISTS idx_sales_norm_party_brand_fy ON sales_data (norm_party, norm_brand, fy);
CREATE INDEX IF NOT EXISTS idx_sales_fy_month ON sales_data (fy, month);

-- 3) Foreign-key side indexes for reference tables.
CREATE INDEX IF NOT EXISTS idx_ref_states_region_id ON ref_states(region_id);
CREATE INDEX IF NOT EXISTS idx_ref_districts_state_id ON ref_districts(state_id);
CREATE INDEX IF NOT EXISTS idx_ref_cities_district_id ON ref_cities(district_id);

-- 4) Trigram indexes for ILIKE-heavy search paths.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_sales_bill_no_trgm ON sales_data USING gin (bill_no gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_sales_to_party_name_trgm ON sales_data USING gin (to_party_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_sales_agent_name_trgm ON sales_data USING gin (agent_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_sales_item_no_trgm ON sales_data USING gin (item_no gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_sales_party_grouped_trgm ON sales_data USING gin (party_grouped gin_trgm_ops);

COMMIT;
