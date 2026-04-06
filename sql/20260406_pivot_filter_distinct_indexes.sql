-- Pivot filter dropdowns: DISTINCT BTRIM(col) on large sales_data.
-- Without expression indexes Postgres often seq-scans the heap for every filter field.
-- Run after deploy (CONCURRENTLY = safe on live DB; takes time on first run):
--   psql "$DATABASE_URL" -f backend/sql/20260406_pivot_filter_distinct_indexes.sql

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_data_branch_btrim
  ON sales_data ((btrim(branch::text)))
  WHERE branch IS NOT NULL AND btrim(branch::text) <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_data_brand_btrim
  ON sales_data ((btrim(brand::text)))
  WHERE brand IS NOT NULL AND btrim(brand::text) <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_data_state_btrim
  ON sales_data ((btrim(state::text)))
  WHERE state IS NOT NULL AND btrim(state::text) <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_data_region_btrim
  ON sales_data ((btrim(region::text)))
  WHERE region IS NOT NULL AND btrim(region::text) <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_data_city_btrim
  ON sales_data ((btrim(city::text)))
  WHERE city IS NOT NULL AND btrim(city::text) <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_data_fy_btrim
  ON sales_data ((btrim(fy::text)))
  WHERE fy IS NOT NULL AND btrim(fy::text) <> '';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sales_data_month_btrim
  ON sales_data ((btrim(month::text)))
  WHERE month IS NOT NULL AND btrim(month::text) <> '';

ANALYZE sales_data;
