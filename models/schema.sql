-- =============================================================================
-- Sales Report System - Database Schema
-- =============================================================================
-- Excel Header to Column Mapping (Reference):
--   Branch            -> branch / ref_branches
--   FY                -> fy
--   MONTH             -> month
--   MMM               -> mmm
--   REGION            -> region / ref_regions
--   STATE             -> state / ref_states
--   DISTRICT          -> district / ref_districts
--   CITY              -> city / ref_cities
--   TYPE OF Business  -> business_type / ref_business_types
--   Agent Names Corr  -> agent_names_correction
--   PARTY GROUPED     -> party_grouped / ref_parties
--   PARTY NAME COUNT  -> party_name_for_count
--   BRAND             -> brand / ref_brands
--   AGENT NAME        -> agent_name / ref_agents
--   TO PARTY NAME     -> to_party_name
--   BILL NO.          -> bill_no
--   BILL Date         -> bill_date
--   ITEM NAME / ITEM NO -> item_no / ref_items
--   SHADE NAME        -> shade_name
--   RATE/UNIT         -> rate_unit
--   SIZE              -> size
--   UNITS/PACK        -> units_pack
--   SL QTY            -> sl_qty
--   GROSS AMOUNT      -> gross_amount
--   AMOUNT BEFORE TAX -> amount_before_tax
--   NET AMOUNT        -> net_amount
--   SALE ORDER NO.    -> sale_order_no
--   SALE ORDER Date   -> sale_order_date
--   Item with Shade   -> item_with_shade
--   Item Category     -> item_category / ref_item_categories
--   Item Sub cat      -> item_sub_cat
--   SO TYPE           -> so_type
--   SCHEME            -> scheme
--   GOODS TYPE        -> goods_type
--   AGENT NAME.       -> agent_name_final / ref_agents
--   PIN CODE          -> pin_code
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Reference Tables (for dropdowns, validation, and optional FK integrity)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ref_regions (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_states (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) UNIQUE NOT NULL,
  region_id INTEGER REFERENCES ref_regions(id),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_branches (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_districts (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  state_id INTEGER REFERENCES ref_states(id),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_cities (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  district_id INTEGER REFERENCES ref_districts(id),
  pin_code VARCHAR(20),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_business_types (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_brands (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_agents (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_parties (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Customer Type Master: maps PARTY NAME -> TYPE (DISTRIBUTOR, DEALER, RETAILER, etc.)
-- Used to derive TYPE OF BUSINESS from TO PARTY NAME during Excel import
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customer_type_master (
  id SERIAL PRIMARY KEY,
  party_name VARCHAR(500) NOT NULL,
  type VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Agent Name Master: maps AGENT NAME -> Combined Name
-- Match rule:
--   sales_data.agent_name = agent_name_master."Agent Name"
-- Result:
--   sales_data.agent_names_correction = agent_name_master."Combined Name"
--   sales_data.agent_name_final       = agent_name_master."Combined Name"
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_name_master (
  id SERIAL PRIMARY KEY,
  "Agent Name" VARCHAR(500) NOT NULL,
  "Combined Name" VARCHAR(500) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_name_master_agent_name_lower
  ON agent_name_master (LOWER(TRIM("Agent Name")));

-- Migration: add party_name if table existed with different schema (e.g. old deploy)
ALTER TABLE customer_type_master ADD COLUMN IF NOT EXISTS party_name VARCHAR(500);

CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_type_master_party_name_lower
  ON customer_type_master (LOWER(TRIM(party_name)));

-- -----------------------------------------------------------------------------
-- Party Grouping Master: 3 columns only. Match: sales_data.to_party_name = party_grouping_master.to_party_name
-- Columns: to_party_name (TO PARTY NAME), party_grouped (PARTY GROUPED), party_name_for_count (PARTY NAME FOR COUNT)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS party_grouping_master (
  id SERIAL PRIMARY KEY,
  to_party_name VARCHAR(500) NOT NULL,
  party_grouped VARCHAR(500),
  party_name_for_count VARCHAR(500),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_party_grouping_master_to_party_name_lower
  ON party_grouping_master (LOWER(TRIM(to_party_name)));

CREATE TABLE IF NOT EXISTS ref_item_categories (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) UNIQUE NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_items (
  id SERIAL PRIMARY KEY,
  name VARCHAR(500) NOT NULL,
  shade_name VARCHAR(255),
  category_id INTEGER REFERENCES ref_item_categories(id),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- Sales Data (Fact Table) - Denormalized for fast bulk import
-- Optional: Add FK columns to reference tables for integrity (slower imports)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sales_data (
  id BIGSERIAL PRIMARY KEY,
  -- Location (Excel: Branch, Region, State, District, City, PIN CODE)
  branch VARCHAR(255),
  fy VARCHAR(50),
  month VARCHAR(50),
  mmm VARCHAR(50),
  region VARCHAR(255),
  state VARCHAR(255),
  district VARCHAR(255),
  city VARCHAR(255),
  pin_code VARCHAR(20),
  -- Business & Party (Excel: TYPE OF Business, PARTY GROUPED, PARTY NAME FOR COUNT, TO PARTY NAME)
  business_type VARCHAR(255),
  agent_names_correction VARCHAR(255),
  party_grouped VARCHAR(255),
  party_name_for_count VARCHAR(255),
  to_party_name VARCHAR(255),
  -- Product & Agent (Excel: BRAND, AGENT NAME, AGENT NAME.)
  brand VARCHAR(255),
  agent_name VARCHAR(255),
  agent_name_final VARCHAR(255),
  -- Bill (Excel: BILL NO., BILL Date)
  bill_no VARCHAR(255),
  bill_date DATE,
  -- Item (Excel: ITEM NAME / ITEM NO, SHADE NAME, Item with Shade, Item Category, Item Sub cat)
  item_no VARCHAR(500),
  shade_name VARCHAR(255),
  item_with_shade VARCHAR(500),
  item_category VARCHAR(255),
  item_sub_cat VARCHAR(255),
  -- Quantities & Amounts (Excel: RATE/UNIT, SIZE, UNITS/PACK, SL QTY, GROSS AMOUNT, AMOUNT BEFORE TAX, NET AMOUNT)
  rate_unit DECIMAL(15, 4),
  size VARCHAR(100),
  units_pack VARCHAR(100),
  sl_qty DECIMAL(15, 4),
  gross_amount DECIMAL(15, 4),
  amount_before_tax DECIMAL(15, 4),
  net_amount DECIMAL(15, 4),
  -- Sale Order (Excel: SALE ORDER NO., SALE ORDER Date)
  sale_order_no VARCHAR(255),
  sale_order_date DATE,
  -- Metadata (Excel: SO TYPE, SCHEME, GOODS TYPE)
  so_type VARCHAR(255),
  scheme VARCHAR(255),
  goods_type VARCHAR(255),
  -- System
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  -- Duplicates allowed: each row gets unique id; no UNIQUE on (bill_no, item_no)
);

-- -----------------------------------------------------------------------------
-- Indexes - Aligned with Excel headers and query patterns
-- -----------------------------------------------------------------------------

-- Allow duplicates: drop unique constraint if it exists (run on existing DBs)
ALTER TABLE sales_data DROP CONSTRAINT IF EXISTS uq_sales_data_bill_item;

-- Primary lookup & filter (required fields + common filters)
CREATE INDEX IF NOT EXISTS idx_sales_data_bill_no ON sales_data (bill_no);
CREATE INDEX IF NOT EXISTS idx_sales_data_bill_date ON sales_data (bill_date);
CREATE INDEX IF NOT EXISTS idx_sales_data_item_no ON sales_data (item_no);
CREATE INDEX IF NOT EXISTS idx_sales_data_net_amount ON sales_data (net_amount);

-- Location filters (Excel: Branch, Region, State, District, City)
CREATE INDEX IF NOT EXISTS idx_sales_data_branch ON sales_data (branch);
CREATE INDEX IF NOT EXISTS idx_sales_data_region ON sales_data (region);
CREATE INDEX IF NOT EXISTS idx_sales_data_state ON sales_data (state);
CREATE INDEX IF NOT EXISTS idx_sales_data_district ON sales_data (district);
CREATE INDEX IF NOT EXISTS idx_sales_data_city ON sales_data (city);

-- Agent & party (Excel: AGENT NAME, AGENT NAME., PARTY GROUPED)
CREATE INDEX IF NOT EXISTS idx_sales_data_agent_name ON sales_data (agent_name);
CREATE INDEX IF NOT EXISTS idx_sales_data_agent_name_final ON sales_data (agent_name_final);
CREATE INDEX IF NOT EXISTS idx_sales_data_party_grouped ON sales_data (party_grouped);
CREATE INDEX IF NOT EXISTS idx_sales_data_to_party_name ON sales_data (to_party_name);

-- Period & reporting (Excel: FY, MONTH, MMM)
CREATE INDEX IF NOT EXISTS idx_sales_data_fy ON sales_data (fy);
CREATE INDEX IF NOT EXISTS idx_sales_data_month ON sales_data (month);
CREATE INDEX IF NOT EXISTS idx_sales_data_mmm ON sales_data (mmm);

-- Product & amounts (Excel: BRAND, GROSS AMOUNT, RATE/UNIT, SL QTY)
CREATE INDEX IF NOT EXISTS idx_sales_data_brand ON sales_data (brand);
CREATE INDEX IF NOT EXISTS idx_sales_data_gross_amount ON sales_data (gross_amount);

-- Composite indexes for common query combinations (pivot filters + GROUP BY)
CREATE INDEX IF NOT EXISTS idx_sales_data_state_brand_bill_date ON sales_data (state, brand, bill_date);
CREATE INDEX IF NOT EXISTS idx_sales_data_state_bill_date ON sales_data (state, bill_date);
CREATE INDEX IF NOT EXISTS idx_sales_data_branch_bill_date ON sales_data (branch, bill_date);
CREATE INDEX IF NOT EXISTS idx_sales_data_item_bill_date ON sales_data (item_no, bill_date);
CREATE INDEX IF NOT EXISTS idx_sales_data_agent_bill_date ON sales_data (agent_name, bill_date);

-- Text search: btree supports prefix/range; for ILIKE '%x%' consider pg_trgm extension
CREATE INDEX IF NOT EXISTS idx_sales_data_item_no_lower ON sales_data (lower(item_no));

-- -----------------------------------------------------------------------------
-- Import Jobs (progress tracking)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS import_jobs (
  id VARCHAR(36) PRIMARY KEY,
  filename VARCHAR(500),
  file_size BIGINT,
  total_rows INTEGER DEFAULT 0,
  processed_rows INTEGER DEFAULT 0,
  failed_rows INTEGER DEFAULT 0,
  status VARCHAR(50) DEFAULT 'pending',
  error_message TEXT,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  cancelled BOOLEAN DEFAULT FALSE
);

-- Runtime import fields (also applied by ensureImportRuntimeSchema); keeps PostgREST/schema in sync when migrated
ALTER TABLE import_jobs
  ADD COLUMN IF NOT EXISTS checkpoint_row INTEGER DEFAULT 0,
  ADD COLUMN IF NOT EXISTS throughput_rps NUMERIC(12,2) DEFAULT 0,
  ADD COLUMN IF NOT EXISTS worker_id VARCHAR(128),
  ADD COLUMN IF NOT EXISTS queued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_import_jobs_status ON import_jobs (status);
CREATE INDEX IF NOT EXISTS idx_import_jobs_created_at ON import_jobs (created_at DESC);

-- -----------------------------------------------------------------------------
-- Import Errors (failed rows)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS import_errors (
  id SERIAL PRIMARY KEY,
  job_id VARCHAR(36) NOT NULL REFERENCES import_jobs(id) ON DELETE CASCADE,
  row_number INTEGER NOT NULL,
  row_data JSONB,
  error_message TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_import_errors_job_id ON import_errors (job_id);

-- -----------------------------------------------------------------------------
-- Import History (audit)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS import_history (
  id SERIAL PRIMARY KEY,
  job_id VARCHAR(36) REFERENCES import_jobs(id),
  filename VARCHAR(500),
  total_rows INTEGER,
  processed_rows INTEGER,
  failed_rows INTEGER,
  status VARCHAR(50),
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  duration_ms BIGINT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_import_history_job_id ON import_history (job_id);
CREATE INDEX IF NOT EXISTS idx_import_history_created_at ON import_history (created_at DESC);

-- -----------------------------------------------------------------------------
-- App Users (basic login)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS app_users (
  id BIGSERIAL PRIMARY KEY,
  email VARCHAR(255) UNIQUE NOT NULL,
  password VARCHAR(255) NOT NULL,
  full_name VARCHAR(255),
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO app_users (email, password, full_name, is_active)
VALUES
  ('vishal@rishabworld.com', 'Vishal@123', 'Vishal', TRUE),
  ('greshma@rishabworld.com', 'Greshma@123', 'Greshma', TRUE)
ON CONFLICT (email) DO NOTHING;

-- -----------------------------------------------------------------------------
-- RLS Policies (Supabase anon key access)
-- Run these if using Supabase with ANON_KEY
-- -----------------------------------------------------------------------------
ALTER TABLE sales_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE import_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE import_errors ENABLE ROW LEVEL SECURITY;
ALTER TABLE app_users ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_name_master ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow anon full access to sales_data" ON sales_data FOR ALL TO anon USING (true) WITH CHECK (true);
CREATE POLICY "Allow anon full access to import_jobs" ON import_jobs FOR ALL TO anon USING (true) WITH CHECK (true);
CREATE POLICY "Allow anon full access to import_errors" ON import_errors FOR ALL TO anon USING (true) WITH CHECK (true);
CREATE POLICY "Allow anon full access to customer_type_master" ON customer_type_master FOR ALL TO anon USING (true) WITH CHECK (true);
CREATE POLICY "Allow anon full access to party_grouping_master" ON party_grouping_master FOR ALL TO anon USING (true) WITH CHECK (true);
CREATE POLICY "Allow anon full access to app_users" ON app_users FOR ALL TO anon USING (true) WITH CHECK (true);
CREATE POLICY "Allow anon full access to agent_name_master" ON agent_name_master FOR ALL TO anon USING (true) WITH CHECK (true);

-- -----------------------------------------------------------------------------
-- Fast lookup views (for instant filter dropdowns)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_distinct_states AS
SELECT DISTINCT state FROM sales_data WHERE state IS NOT NULL ORDER BY state;

-- -----------------------------------------------------------------------------
-- region_master view: state → region from ref_states + ref_regions
-- Backend reads region_master(state, region) when present; otherwise uses ref_states join.
-- Idempotent for view-only; does not drop an existing table named region_master.
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS region_master;
CREATE VIEW region_master AS
SELECT s.name AS state, r.name AS region
FROM ref_states s
JOIN ref_regions r ON r.id = s.region_id;

-- If you use a physical TABLE region_master (uuid, state, region) instead of this view:
-- 1) Do not run DROP VIEW / CREATE VIEW above against that DB (name conflict).
-- 2) If Supabase REST returns empty or "fetch failed", ensure DATABASE_URL is set — backend falls back to SQL.
-- 3) For anon key reads via PostgREST, enable RLS + policy, e.g.:
--    ALTER TABLE region_master ENABLE ROW LEVEL SECURITY;
--    DROP POLICY IF EXISTS "Allow anon select region_master" ON region_master;
--    CREATE POLICY "Allow anon select region_master" ON region_master FOR SELECT TO anon USING (true);

-- -----------------------------------------------------------------------------
-- SO Master (per brand + FY): PARTY NAME → TYPE OF ORDER; import maps so_type by key
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS so_master (
  id SERIAL PRIMARY KEY,
  party_name TEXT NOT NULL,
  type_of_order TEXT NOT NULL,
  brand TEXT,
  fy TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE so_master ADD COLUMN IF NOT EXISTS brand TEXT;
ALTER TABLE so_master ADD COLUMN IF NOT EXISTS fy TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'unique_so'
  ) THEN
    ALTER TABLE so_master
    ADD CONSTRAINT unique_so UNIQUE (party_name, brand, fy);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_so_master_lookup ON so_master (party_name, brand, fy);

-- sales_data.fy may already exist from earlier schema; safe no-op if present
ALTER TABLE sales_data ADD COLUMN IF NOT EXISTS fy TEXT;

-- -----------------------------------------------------------------------------
-- Admin Master Edit History (row-level audit)
-- Separate tables for event metadata, previous value, and new value
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS admin_master_table_row_edit_history_event (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  master_table TEXT NOT NULL,
  row_uuid TEXT NOT NULL,
  column_name TEXT NOT NULL,
  edited_by TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_admin_master_row_edit_history_created_at
  ON admin_master_table_row_edit_history_event (created_at DESC);


CREATE TABLE IF NOT EXISTS admin_master_table_row_edit_history_before (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id UUID NOT NULL REFERENCES admin_master_table_row_edit_history_event(id) ON DELETE CASCADE,
  previous_value TEXT
);

CREATE INDEX IF NOT EXISTS idx_admin_master_row_edit_before_event_id
  ON admin_master_table_row_edit_history_before (event_id);

CREATE TABLE IF NOT EXISTS admin_master_table_row_edit_history_after (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id UUID NOT NULL REFERENCES admin_master_table_row_edit_history_event(id) ON DELETE CASCADE,
  new_value TEXT
);

CREATE INDEX IF NOT EXISTS idx_admin_master_row_edit_after_event_id
  ON admin_master_table_row_edit_history_after (event_id);