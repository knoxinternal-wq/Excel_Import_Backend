-- Performance indexes for large-table filtering/pagination/search.
-- Safe to run repeatedly.

-- Optional trigram support for ilike/contains text search.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Keyset and sort acceleration.
CREATE INDEX IF NOT EXISTS idx_sales_id_desc ON sales_data (id DESC);

-- Common filter + date-sort path (bill_date is the fact date field).
CREATE INDEX IF NOT EXISTS idx_sales_state_bill_date_desc ON sales_data (state, bill_date DESC);

-- Party grouped filtering.
CREATE INDEX IF NOT EXISTS idx_sales_party_grouped ON sales_data (party_grouped);

-- Text search acceleration.
CREATE INDEX IF NOT EXISTS idx_sales_party_grouped_trgm
  ON sales_data USING gin (party_grouped gin_trgm_ops);

