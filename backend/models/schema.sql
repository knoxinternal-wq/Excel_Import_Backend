-- =============================================================================
-- Sales Reporting Platform - Single Source Schema
-- =============================================================================
-- Notes:
-- 1) This is a clean-install schema (single-file architecture).
-- 2) Keeps business-compatible table/column names used by current backend.
-- 3) Designed for import throughput + pivot analytics (50L+ rows).
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- -----------------------------------------------------------------------------
-- Reference tables
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
  name VARCHAR(255),
  state_id INTEGER REFERENCES ref_states(id),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_cities (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  district_id INTEGER REFERENCES ref_districts(id),
  pin_code VARCHAR(20),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_business_types (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_brands (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_agents (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ref_parties (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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

CREATE INDEX IF NOT EXISTS idx_ref_states_region_id ON ref_states(region_id);
CREATE INDEX IF NOT EXISTS idx_ref_districts_state_id ON ref_districts(state_id);
CREATE INDEX IF NOT EXISTS idx_ref_cities_district_id ON ref_cities(district_id);

-- -----------------------------------------------------------------------------
-- Master tables
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS customer_type_master (
  id SERIAL PRIMARY KEY,
  party_name VARCHAR(500),
  type VARCHAR(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_customer_type_norm
  ON customer_type_master (LOWER(TRIM(COALESCE(party_name, ''))));

CREATE TABLE IF NOT EXISTS agent_name_master (
  id SERIAL PRIMARY KEY,
  "Agent Name" VARCHAR(500),
  "Combined Name" VARCHAR(500),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_agent_norm
  ON agent_name_master (LOWER(TRIM(COALESCE("Agent Name", ''))));

CREATE TABLE IF NOT EXISTS party_grouping_master (
  id SERIAL PRIMARY KEY,
  to_party_name VARCHAR(500),
  party_grouped VARCHAR(500),
  party_name_for_count VARCHAR(500),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_party_group_norm
  ON party_grouping_master (LOWER(TRIM(COALESCE(to_party_name, ''))));

-- Required by district/pincode enrichment flow
CREATE TABLE IF NOT EXISTS party_master_app (
  "S.NO." INTEGER,
  "ACCOUNT_NAME" TEXT,
  "PRINT_NAME" TEXT,
  "GST_NO." TEXT,
  "GST_DATE" TEXT,
  "GST_STATE_CODE" TEXT,
  "GST_STATE_NAME" TEXT,
  "PAN_NO." TEXT,
  "CONTACT_PERSON" TEXT,
  "MOBILE_NO." TEXT,
  "FIRST_ADDRESS1" TEXT,
  "FIRST_ADDRESS2" TEXT,
  "FIRST_ADDRESS3" TEXT,
  "PIN_CODE" TEXT,
  "FIRST_CITY_NAME" TEXT,
  "STATE" TEXT,
  "COUNTRY" TEXT,
  "CONTACT_NO." TEXT,
  "TELEPHONE(OFF)" TEXT,
  "GROUP1.GRP1" TEXT,
  "GROUP4.GRP1" TEXT,
  "E-MAIL" TEXT,
  "TRANSPORTER_NAME" TEXT,
  "CREDIT_DAYS" INTEGER,
  "ACCOUNT_CODE" TEXT,
  "CREATION_DATE" TEXT,
  "MODIFY_DATE" TEXT,
  "TDS_FORM_NAME" TEXT,
  "AADHAR_NO." TEXT,
  "DISTRICT" TEXT
);
CREATE INDEX IF NOT EXISTS idx_party_master_account_name
  ON party_master_app (LOWER(TRIM(COALESCE("ACCOUNT_NAME", ''))));
CREATE INDEX IF NOT EXISTS idx_party_master_state
  ON party_master_app (LOWER(TRIM(COALESCE("STATE", ''))));
CREATE INDEX IF NOT EXISTS idx_party_master_city
  ON party_master_app (LOWER(TRIM(COALESCE("FIRST_CITY_NAME", ''))));

-- -----------------------------------------------------------------------------
-- Fact table (partitioned)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sales_data (
  id BIGSERIAL,

  branch VARCHAR(255),
  fy VARCHAR(50),
  month VARCHAR(50),
  mmm VARCHAR(50),
  region VARCHAR(255),
  state VARCHAR(255),
  district VARCHAR(255),
  city VARCHAR(255),
  pin_code VARCHAR(20),

  business_type VARCHAR(255),
  agent_names_correction VARCHAR(255),
  party_grouped VARCHAR(255),
  party_name_for_count VARCHAR(255),
  to_party_name VARCHAR(255),

  brand VARCHAR(255),
  agent_name VARCHAR(255),
  agent_name_final VARCHAR(255),

  bill_no VARCHAR(255),
  bill_date DATE,

  item_no VARCHAR(500),
  shade_name VARCHAR(255),
  item_with_shade VARCHAR(500),
  item_category VARCHAR(255),
  item_sub_cat VARCHAR(255),

  rate_unit DECIMAL(15,4),
  size VARCHAR(100),
  units_pack VARCHAR(100),
  sl_qty DECIMAL(15,4),

  gross_amount DECIMAL(15,4),
  amount_before_tax DECIMAL(15,4),
  net_amount DECIMAL(15,4),

  sale_order_no VARCHAR(255),
  sale_order_date DATE,

  so_type VARCHAR(255),
  scheme VARCHAR(255),
  goods_type VARCHAR(255),

  norm_party TEXT GENERATED ALWAYS AS (UPPER(TRIM(COALESCE(to_party_name, '')))) STORED,
  norm_brand TEXT GENERATED ALWAYS AS (UPPER(TRIM(COALESCE(brand, '')))) STORED,
  norm_agent TEXT GENERATED ALWAYS AS (UPPER(TRIM(COALESCE(agent_name, '')))) STORED,
  norm_state TEXT GENERATED ALWAYS AS (UPPER(TRIM(COALESCE(state, '')))) STORED,

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (bill_date);

CREATE TABLE IF NOT EXISTS sales_data_fy_2020_21 PARTITION OF sales_data
  FOR VALUES FROM ('2020-04-01') TO ('2021-04-01');
CREATE TABLE IF NOT EXISTS sales_data_fy_2021_22 PARTITION OF sales_data
  FOR VALUES FROM ('2021-04-01') TO ('2022-04-01');
CREATE TABLE IF NOT EXISTS sales_data_fy_2022_23 PARTITION OF sales_data
  FOR VALUES FROM ('2022-04-01') TO ('2023-04-01');
CREATE TABLE IF NOT EXISTS sales_data_fy_2023_24 PARTITION OF sales_data
  FOR VALUES FROM ('2023-04-01') TO ('2024-04-01');
CREATE TABLE IF NOT EXISTS sales_data_fy_2024_25 PARTITION OF sales_data
  FOR VALUES FROM ('2024-04-01') TO ('2025-04-01');
CREATE TABLE IF NOT EXISTS sales_data_fy_2025_26 PARTITION OF sales_data
  FOR VALUES FROM ('2025-04-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS sales_data_default PARTITION OF sales_data DEFAULT;

-- Fast COPY target (import pipeline staging)
CREATE UNLOGGED TABLE IF NOT EXISTS sales_data_staging (
  LIKE sales_data_default INCLUDING DEFAULTS INCLUDING GENERATED
);

-- fact indexes
CREATE INDEX IF NOT EXISTS idx_sales_state_brand_date ON sales_data (state, brand, bill_date);
CREATE INDEX IF NOT EXISTS idx_sales_fy_month ON sales_data (fy, month);
CREATE INDEX IF NOT EXISTS idx_sales_norm_party ON sales_data (norm_party);
CREATE INDEX IF NOT EXISTS idx_sales_norm_brand ON sales_data (norm_brand);
CREATE INDEX IF NOT EXISTS idx_sales_norm_state ON sales_data (norm_state);
CREATE INDEX IF NOT EXISTS idx_sales_norm_agent ON sales_data (norm_agent);
CREATE INDEX IF NOT EXISTS idx_sales_norm_party_brand_fy ON sales_data (norm_party, norm_brand, fy);
CREATE INDEX IF NOT EXISTS idx_sales_brin_date ON sales_data USING BRIN (bill_date);
CREATE INDEX IF NOT EXISTS idx_sales_brin_created_at ON sales_data USING BRIN (created_at);
CREATE INDEX IF NOT EXISTS idx_sales_id_desc ON sales_data (id DESC);
CREATE INDEX IF NOT EXISTS idx_sales_party_trgm ON sales_data USING gin (to_party_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_sales_agent_trgm ON sales_data USING gin (agent_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_sales_item_trgm ON sales_data USING gin (item_no gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_sales_bill_no_trgm ON sales_data USING gin (bill_no gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_sales_party_grouped_trgm ON sales_data USING gin (party_grouped gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_sales_data_branch_btrim
  ON sales_data ((BTRIM(branch::text)))
  WHERE branch IS NOT NULL AND BTRIM(branch::text) <> '';
CREATE INDEX IF NOT EXISTS idx_sales_data_brand_btrim
  ON sales_data ((BTRIM(brand::text)))
  WHERE brand IS NOT NULL AND BTRIM(brand::text) <> '';
CREATE INDEX IF NOT EXISTS idx_sales_data_state_btrim
  ON sales_data ((BTRIM(state::text)))
  WHERE state IS NOT NULL AND BTRIM(state::text) <> '';
CREATE INDEX IF NOT EXISTS idx_sales_data_region_btrim
  ON sales_data ((BTRIM(region::text)))
  WHERE region IS NOT NULL AND BTRIM(region::text) <> '';
CREATE INDEX IF NOT EXISTS idx_sales_data_city_btrim
  ON sales_data ((BTRIM(city::text)))
  WHERE city IS NOT NULL AND BTRIM(city::text) <> '';
CREATE INDEX IF NOT EXISTS idx_sales_data_fy_btrim
  ON sales_data ((BTRIM(fy::text)))
  WHERE fy IS NOT NULL AND BTRIM(fy::text) <> '';
CREATE INDEX IF NOT EXISTS idx_sales_data_month_btrim
  ON sales_data ((BTRIM(month::text)))
  WHERE month IS NOT NULL AND BTRIM(month::text) <> '';
CREATE INDEX IF NOT EXISTS idx_sales_data_party_grouped_btrim
  ON sales_data ((BTRIM(COALESCE(party_grouped::text, ''))));
CREATE INDEX IF NOT EXISTS idx_sales_data_agent_name_btrim
  ON sales_data ((BTRIM(COALESCE(agent_name::text, ''))));
CREATE INDEX IF NOT EXISTS idx_sales_data_to_party_name_btrim
  ON sales_data ((BTRIM(COALESCE(to_party_name::text, ''))));
CREATE INDEX IF NOT EXISTS idx_sales_data_state_brand_partial
  ON sales_data (state, brand) WHERE state IS NOT NULL AND brand IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sales_data_brand_month_partial
  ON sales_data (brand, month) WHERE brand IS NOT NULL AND month IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_sales_data_party_grouped_brand_btrim
  ON sales_data ((BTRIM(COALESCE(party_grouped::text, ''))), (BTRIM(COALESCE(brand::text, ''))));

-- -----------------------------------------------------------------------------
-- Operational tables used by backend
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
  cancelled BOOLEAN DEFAULT FALSE,
  checkpoint_row INTEGER DEFAULT 0,
  throughput_rps NUMERIC(12,2) DEFAULT 0,
  worker_id VARCHAR(128),
  queued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  retry_count INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_import_jobs_status ON import_jobs (status);
CREATE INDEX IF NOT EXISTS idx_import_jobs_created_at ON import_jobs (created_at DESC);

CREATE TABLE IF NOT EXISTS import_errors (
  id SERIAL PRIMARY KEY,
  job_id VARCHAR(36) NOT NULL REFERENCES import_jobs(id) ON DELETE CASCADE,
  row_number INTEGER NOT NULL,
  row_data JSONB,
  error_message TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_import_errors_job_id ON import_errors (job_id);

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


-- Admin upload/edit history tables used by adminController
CREATE TABLE IF NOT EXISTS admin_so_master_upload_history (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filename TEXT,
  brand TEXT,
  fy TEXT,
  status TEXT,
  inserted_rows INTEGER DEFAULT 0,
  error_message TEXT,
  uploaded_by TEXT,
  total_rows INTEGER DEFAULT 0,
  upserted_rows INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_admin_so_master_upload_history_created_at
  ON admin_so_master_upload_history (created_at DESC);

CREATE TABLE IF NOT EXISTS admin_dnj_so_master_edit_history (
  id BIGSERIAL PRIMARY KEY,
  party_name TEXT NOT NULL,
  master_table TEXT,
  brand TEXT NOT NULL,
  fy TEXT NOT NULL,
  previous_type_of_order TEXT,
  new_type_of_order TEXT,
  edited_by TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS admin_ic_so_master_edit_history (LIKE admin_dnj_so_master_edit_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
CREATE TABLE IF NOT EXISTS admin_rf_so_master_edit_history (LIKE admin_dnj_so_master_edit_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
CREATE TABLE IF NOT EXISTS admin_vercelli_so_master_edit_history (LIKE admin_dnj_so_master_edit_history INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

-- Generic master row edit history
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


CREATE OR REPLACE VIEW v_distinct_states AS
SELECT DISTINCT state FROM sales_data WHERE state IS NOT NULL ORDER BY state;

-- -----------------------------------------------------------------------------
-- Materialized views (pivot-first hot paths)
-- -----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_sales_state_month;
CREATE MATERIALIZED VIEW mv_sales_state_month AS
SELECT
  state,
  DATE_TRUNC('month', bill_date::timestamp)::date AS month,
  SUM(net_amount) AS total_net,
  SUM(amount_before_tax) AS total_tax,
  SUM(sl_qty) AS total_qty,
  COUNT(*)::bigint AS fact_row_count
FROM sales_data
GROUP BY 1, 2;
CREATE INDEX IF NOT EXISTS idx_mv_state_month
  ON mv_sales_state_month (state, month);

DROP MATERIALIZED VIEW IF EXISTS mv_sales_branch_brand;
CREATE MATERIALIZED VIEW mv_sales_branch_brand AS
SELECT
  branch,
  brand,
  SUM(net_amount) AS total_net,
  SUM(amount_before_tax) AS total_tax,
  SUM(sl_qty) AS total_qty,
  COUNT(*)::bigint AS fact_row_count
FROM sales_data
GROUP BY 1, 2;
CREATE INDEX IF NOT EXISTS idx_mv_branch_brand
  ON mv_sales_branch_brand (branch, brand);

DROP MATERIALIZED VIEW IF EXISTS mv_sales_agent_party_month;
CREATE MATERIALIZED VIEW mv_sales_agent_party_month AS
SELECT
  agent_name,
  to_party_name,
  DATE_TRUNC('month', bill_date::timestamp)::date AS month,
  SUM(net_amount) AS total_net,
  SUM(amount_before_tax) AS total_tax,
  SUM(sl_qty) AS total_qty,
  COUNT(*)::bigint AS fact_row_count
FROM sales_data
GROUP BY 1, 2, 3;
CREATE INDEX IF NOT EXISTS idx_mv_agent_party_month
  ON mv_sales_agent_party_month (agent_name, to_party_name, month);

DROP MATERIALIZED VIEW IF EXISTS mv_sales_party_grouped_brand;
CREATE MATERIALIZED VIEW mv_sales_party_grouped_brand AS
SELECT
  party_grouped,
  brand,
  SUM(net_amount) AS total_net,
  SUM(amount_before_tax) AS total_tax,
  SUM(sl_qty) AS total_qty,
  COUNT(*)::bigint AS fact_row_count
FROM sales_data
GROUP BY 1, 2;
CREATE INDEX IF NOT EXISTS idx_mv_party_grouped_brand
  ON mv_sales_party_grouped_brand (party_grouped, brand);

DROP MATERIALIZED VIEW IF EXISTS mv_sales_state_party_grouped_brand;
CREATE MATERIALIZED VIEW mv_sales_state_party_grouped_brand AS
SELECT
  state,
  party_grouped,
  brand,
  SUM(net_amount) AS total_net,
  SUM(amount_before_tax) AS total_tax,
  SUM(sl_qty) AS total_qty,
  COUNT(*)::bigint AS fact_row_count
FROM sales_data
GROUP BY 1, 2, 3;
CREATE INDEX IF NOT EXISTS idx_mv_state_party_grouped_brand
  ON mv_sales_state_party_grouped_brand (state, party_grouped, brand);

DROP MATERIALIZED VIEW IF EXISTS sales_mv;
CREATE MATERIALIZED VIEW sales_mv AS
SELECT
  state,
  branch,
  brand,
  DATE_TRUNC('month', bill_date::timestamp)::date AS month,
  SUM(net_amount) AS total,
  SUM(amount_before_tax) AS total_tax,
  SUM(sl_qty) AS total_qty,
  COUNT(*)::bigint AS fact_row_count
FROM sales_data
GROUP BY 1, 2, 3, 4;
CREATE UNIQUE INDEX IF NOT EXISTS idx_sales_mv_dims
  ON sales_mv (state, branch, brand, month);

-- -----------------------------------------------------------------------------
-- Supabase RLS policies (anon access expected by this backend)
-- -----------------------------------------------------------------------------
ALTER TABLE sales_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE import_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE import_errors ENABLE ROW LEVEL SECURITY;
ALTER TABLE app_users ENABLE ROW LEVEL SECURITY;
ALTER TABLE customer_type_master ENABLE ROW LEVEL SECURITY;
ALTER TABLE party_grouping_master ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_name_master ENABLE ROW LEVEL SECURITY;
ALTER TABLE party_master_app ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "Allow anon full access to sales_data" ON sales_data;
CREATE POLICY "Allow anon full access to sales_data" ON sales_data FOR ALL TO anon USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Allow anon full access to import_jobs" ON import_jobs;
CREATE POLICY "Allow anon full access to import_jobs" ON import_jobs FOR ALL TO anon USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Allow anon full access to import_errors" ON import_errors;
CREATE POLICY "Allow anon full access to import_errors" ON import_errors FOR ALL TO anon USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Allow anon full access to app_users" ON app_users;
CREATE POLICY "Allow anon full access to app_users" ON app_users FOR ALL TO anon USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Allow anon full access to customer_type_master" ON customer_type_master;
CREATE POLICY "Allow anon full access to customer_type_master" ON customer_type_master FOR ALL TO anon USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Allow anon full access to party_grouping_master" ON party_grouping_master;
CREATE POLICY "Allow anon full access to party_grouping_master" ON party_grouping_master FOR ALL TO anon USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Allow anon full access to agent_name_master" ON agent_name_master;
CREATE POLICY "Allow anon full access to agent_name_master" ON agent_name_master FOR ALL TO anon USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Allow anon full access to party_master_app" ON party_master_app;
CREATE POLICY "Allow anon full access to party_master_app" ON party_master_app FOR ALL TO anon USING (true) WITH CHECK (true);

-- -----------------------------------------------------------------------------
-- SO master tables (independent per brand)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dnj_so_master (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  party_name TEXT,
  so_agent_name TEXT,
  branch TEXT,
  company_name TEXT DEFAULT 'DNJ',
  so_order_no TEXT,
  so_order_date DATE,
  type_of_order TEXT,
  brand TEXT,
  fy TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS ic_so_master (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  party_name TEXT,
  so_agent_name TEXT,
  branch TEXT,
  company_name TEXT DEFAULT 'IC',
  so_order_no TEXT,
  so_order_date DATE,
  type_of_order TEXT,
  brand TEXT,
  fy TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS rf_so_master (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  party_name TEXT,
  so_agent_name TEXT,
  branch TEXT,
  company_name TEXT DEFAULT 'RF',
  so_order_no TEXT,
  so_order_date DATE,
  type_of_order TEXT,
  brand TEXT,
  fy TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS vercelli_so_master (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  party_name TEXT,
  so_agent_name TEXT,
  branch TEXT,
  company_name TEXT DEFAULT 'VERCELLI',
  so_order_no TEXT,
  so_order_date DATE,
  type_of_order TEXT,
  brand TEXT,
  fy TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dnj_lookup ON dnj_so_master (party_name, brand, fy);
CREATE INDEX IF NOT EXISTS idx_dnj_order_date ON dnj_so_master (so_order_date);
CREATE INDEX IF NOT EXISTS idx_dnj_agent ON dnj_so_master (so_agent_name);
CREATE INDEX IF NOT EXISTS idx_dnj_party_trgm ON dnj_so_master USING gin (party_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_ic_lookup ON ic_so_master (party_name, brand, fy);
CREATE INDEX IF NOT EXISTS idx_ic_order_date ON ic_so_master (so_order_date);
CREATE INDEX IF NOT EXISTS idx_ic_agent ON ic_so_master (so_agent_name);
CREATE INDEX IF NOT EXISTS idx_ic_party_trgm ON ic_so_master USING gin (party_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_rf_lookup ON rf_so_master (party_name, brand, fy);
CREATE INDEX IF NOT EXISTS idx_rf_order_date ON rf_so_master (so_order_date);
CREATE INDEX IF NOT EXISTS idx_rf_agent ON rf_so_master (so_agent_name);
CREATE INDEX IF NOT EXISTS idx_rf_party_trgm ON rf_so_master USING gin (party_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_vercelli_lookup ON vercelli_so_master (party_name, brand, fy);
CREATE INDEX IF NOT EXISTS idx_vercelli_order_date ON vercelli_so_master (so_order_date);
CREATE INDEX IF NOT EXISTS idx_vercelli_agent ON vercelli_so_master (so_agent_name);
CREATE INDEX IF NOT EXISTS idx_vercelli_party_trgm ON vercelli_so_master USING gin (party_name gin_trgm_ops);

ALTER TABLE dnj_so_master ENABLE ROW LEVEL SECURITY;
ALTER TABLE ic_so_master ENABLE ROW LEVEL SECURITY;
ALTER TABLE rf_so_master ENABLE ROW LEVEL SECURITY;
ALTER TABLE vercelli_so_master ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Allow anon full access to dnj_so_master" ON dnj_so_master;
CREATE POLICY "Allow anon full access to dnj_so_master" ON dnj_so_master FOR ALL TO anon USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Allow anon full access to ic_so_master" ON ic_so_master;
CREATE POLICY "Allow anon full access to ic_so_master" ON ic_so_master FOR ALL TO anon USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Allow anon full access to rf_so_master" ON rf_so_master;
CREATE POLICY "Allow anon full access to rf_so_master" ON rf_so_master FOR ALL TO anon USING (true) WITH CHECK (true);
DROP POLICY IF EXISTS "Allow anon full access to vercelli_so_master" ON vercelli_so_master;
CREATE POLICY "Allow anon full access to vercelli_so_master" ON vercelli_so_master FOR ALL TO anon USING (true) WITH CHECK (true);

-- -----------------------------------------------------------------------------
-- region_master table (editable in admin + used by loaders)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS region_master (
  id SERIAL PRIMARY KEY,
  state VARCHAR(255) NOT NULL,
  region VARCHAR(255) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE region_master
  ADD COLUMN IF NOT EXISTS state VARCHAR(255),
  ADD COLUMN IF NOT EXISTS region VARCHAR(255),
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'unique_region_master_state'
  ) THEN
    ALTER TABLE region_master
      ADD CONSTRAINT unique_region_master_state UNIQUE (state);
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_region_master_region ON region_master (region);

INSERT INTO region_master (state, region)
SELECT s.name AS state, r.name AS region
FROM ref_states s
JOIN ref_regions r ON r.id = s.region_id
ON CONFLICT (state) DO NOTHING;

ALTER TABLE region_master ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "Allow anon full access to region_master" ON region_master;
CREATE POLICY "Allow anon full access to region_master" ON region_master FOR ALL TO anon USING (true) WITH CHECK (true);

ANALYZE sales_data;

ALTER TABLE app_users
  ADD COLUMN IF NOT EXISTS is_admin BOOLEAN DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMP;