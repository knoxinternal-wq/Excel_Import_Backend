-- =============================================================================
-- Enterprise pivot helpers (run with DATABASE_URL after schema.sql)
-- =============================================================================
-- 1) Backfill agent display columns (matches import: LOWER(TRIM) join on master)
-- 2) Materialized view: brand × state × month (used when pivot rows are exactly that order)
-- 3) Partial indexes for common filtered scans
--
-- Refresh MV after bulk imports:
--   REFRESH MATERIALIZED VIEW mv_sales_brand_state_month;
-- Or: npm run db:refresh-pivot-mv
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1) Backfill agent_name_final / agent_names_correction (legacy rows)
-- -----------------------------------------------------------------------------
UPDATE sales_data sd
SET
  agent_name_final = sub.dn,
  agent_names_correction = sub.dn
FROM (
  SELECT
    sd2.id,
    COALESCE(
      (
        SELECT m."Combined Name"
        FROM agent_name_master m
        WHERE LOWER(TRIM(m."Agent Name")) = LOWER(TRIM(COALESCE(sd2.agent_name::text, '')))
        LIMIT 1
      ),
      NULLIF(TRIM(COALESCE(sd2.agent_name::text, '')), '')
    ) AS dn
  FROM sales_data sd2
) sub
WHERE sd.id = sub.id
  AND sub.dn IS NOT NULL
  AND (
    sd.agent_name_final IS NULL
    OR TRIM(COALESCE(sd.agent_name_final::text, '')) = ''
    OR sd.agent_names_correction IS NULL
    OR TRIM(COALESCE(sd.agent_names_correction::text, '')) = ''
  );

-- -----------------------------------------------------------------------------
-- 2) Materialized view: brand × state × month (detail grain only)
-- -----------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS mv_sales_brand_state_month;

CREATE MATERIALIZED VIEW mv_sales_brand_state_month AS
SELECT
  (CASE WHEN sd.brand IS NULL OR sd.brand::text = '' THEN NULL ELSE sd.brand::text END) AS brand,
  (CASE WHEN sd.state IS NULL OR sd.state::text = '' THEN NULL ELSE sd.state::text END) AS state,
  (CASE WHEN sd.month IS NULL OR sd.month::text = '' THEN NULL ELSE sd.month::text END) AS month,
  SUM(sd.net_amount)::double precision AS sum_net_amount,
  COUNT(*)::bigint AS fact_row_count
FROM sales_data sd
GROUP BY 1, 2, 3;

CREATE INDEX IF NOT EXISTS idx_mv_bs_month_brand ON mv_sales_brand_state_month (brand);
CREATE INDEX IF NOT EXISTS idx_mv_bs_month_state ON mv_sales_brand_state_month (state);
CREATE INDEX IF NOT EXISTS idx_mv_bs_month_month ON mv_sales_brand_state_month (month);
CREATE INDEX IF NOT EXISTS idx_mv_bs_month_bsm ON mv_sales_brand_state_month (brand, state, month);

-- -----------------------------------------------------------------------------
-- 3) Partial indexes on sales_data (examples — adjust to your filter workload)
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_sales_data_state_brand_partial
  ON sales_data (state, brand)
  WHERE state IS NOT NULL AND brand IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_sales_data_brand_month_partial
  ON sales_data (brand, month)
  WHERE brand IS NOT NULL AND month IS NOT NULL;

-- -----------------------------------------------------------------------------
-- Optional: GROUPING SETS / ROLLUP for ad-hoc SQL reports (not used by API JSON)
-- Example subtotal rows in one scan:
--
-- SELECT brand, state, month,
--        SUM(net_amount) AS sum_net,
--        COUNT(*) AS n
-- FROM sales_data
-- GROUP BY ROLLUP (brand, state, month);
-- =============================================================================
