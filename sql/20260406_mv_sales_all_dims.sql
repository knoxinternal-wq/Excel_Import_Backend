-- Pre-aggregated pivot source for large `sales_data` (millions of rows).
-- Run manually on Postgres/Supabase:
--   psql "$DATABASE_URL" -f backend/sql/20260406_mv_sales_all_dims.sql

DROP MATERIALIZED VIEW IF EXISTS mv_sales_all_dims;

CREATE MATERIALIZED VIEW mv_sales_all_dims AS
SELECT
  btrim(branch::text) AS branch,
  btrim(region::text) AS region,
  btrim(state::text) AS state,
  btrim(district::text) AS district,
  btrim(city::text) AS city,
  btrim(brand::text) AS brand,
  btrim(party_grouped::text) AS party_grouped,
  btrim(agent_name::text) AS agent_name,
  btrim(fy::text) AS fy,
  btrim(month::text) AS month,
  btrim(mmm::text) AS mmm,
  btrim(so_type::text) AS so_type,
  btrim(item_category::text) AS item_category,
  btrim(item_sub_cat::text) AS item_sub_cat,
  btrim(goods_type::text) AS goods_type,
  SUM(COALESCE(net_amount, 0)) AS sum_net_amount,
  SUM(COALESCE(sl_qty, 0)) AS sum_sl_qty,
  COUNT(*)::bigint AS row_count
FROM sales_data
WHERE NOT (
  lower(btrim(coalesce(branch::text, ''))) = 'total'
  OR lower(btrim(coalesce(branch::text, ''))) = 'grand total'
  OR lower(btrim(coalesce(branch::text, ''))) LIKE '%grand total%'
  OR lower(btrim(coalesce(branch::text, ''))) LIKE '%grandtotal%'
)
GROUP BY
  btrim(branch::text),
  btrim(region::text),
  btrim(state::text),
  btrim(district::text),
  btrim(city::text),
  btrim(brand::text),
  btrim(party_grouped::text),
  btrim(agent_name::text),
  btrim(fy::text),
  btrim(month::text),
  btrim(mmm::text),
  btrim(so_type::text),
  btrim(item_category::text),
  btrim(item_sub_cat::text),
  btrim(goods_type::text);

CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_branch ON mv_sales_all_dims (branch);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_region ON mv_sales_all_dims (region);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_state ON mv_sales_all_dims (state);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_district ON mv_sales_all_dims (district);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_city ON mv_sales_all_dims (city);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_brand ON mv_sales_all_dims (brand);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_party_grouped ON mv_sales_all_dims (party_grouped);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_agent_name ON mv_sales_all_dims (agent_name);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_fy ON mv_sales_all_dims (fy);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_month ON mv_sales_all_dims (month);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_mmm ON mv_sales_all_dims (mmm);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_so_type ON mv_sales_all_dims (so_type);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_item_category ON mv_sales_all_dims (item_category);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_item_sub_cat ON mv_sales_all_dims (item_sub_cat);
CREATE INDEX IF NOT EXISTS idx_mv_sales_all_dims_goods_type ON mv_sales_all_dims (goods_type);

ANALYZE mv_sales_all_dims;

-- Refresh command after large imports:
-- REFRESH MATERIALIZED VIEW mv_sales_all_dims;
