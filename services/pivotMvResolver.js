const DEFAULT_REGISTRY = Object.freeze([
  {
    name: 'mv_sales_state_month',
    relationEnv: 'PIVOT_MV_STATE_MONTH',
    defaultRelation: 'mv_sales_state_month',
    dimensions: ['state', 'month'],
    filterDimensions: ['state', 'month'],
    measureMap: {
      net_amount: 'total_net',
      amount_before_tax: 'total_tax',
      sl_qty: 'total_qty',
    },
    disableGrandTotalBranchExclusion: true,
    monthDimensions: ['month'],
  },
  {
    name: 'mv_sales_branch_brand',
    relationEnv: 'PIVOT_MV_BRANCH_BRAND',
    defaultRelation: 'mv_sales_branch_brand',
    dimensions: ['branch', 'brand'],
    filterDimensions: ['branch', 'brand'],
    measureMap: {
      net_amount: 'total_net',
      amount_before_tax: 'total_tax',
      sl_qty: 'total_qty',
    },
  },
  {
    name: 'mv_sales_party_grouped_brand',
    relationEnv: 'PIVOT_MV_PARTY_GROUPED_BRAND',
    defaultRelation: 'mv_sales_party_grouped_brand',
    dimensions: ['party_grouped', 'brand'],
    filterDimensions: ['party_grouped', 'brand'],
    measureMap: {
      net_amount: 'total_net',
      amount_before_tax: 'total_tax',
      sl_qty: 'total_qty',
    },
    disableGrandTotalBranchExclusion: true,
  },
  {
    name: 'mv_sales_state_party_grouped_brand',
    relationEnv: 'PIVOT_MV_STATE_PARTY_GROUPED_BRAND',
    defaultRelation: 'mv_sales_state_party_grouped_brand',
    dimensions: ['state', 'party_grouped', 'brand'],
    filterDimensions: ['state', 'party_grouped', 'brand'],
    measureMap: {
      net_amount: 'total_net',
      amount_before_tax: 'total_tax',
      sl_qty: 'total_qty',
    },
    disableGrandTotalBranchExclusion: true,
  },
  {
    name: 'mv_sales_agent_final_branch',
    relationEnv: 'PIVOT_MV_AGENT_FINAL_BRANCH',
    defaultRelation: 'mv_sales_agent_final_branch',
    dimensions: ['agent_name_final', 'branch'],
    filterDimensions: ['agent_name_final', 'branch'],
    measureMap: {
      net_amount: 'total_net',
      amount_before_tax: 'total_tax',
      sl_qty: 'total_qty',
    },
  },
  {
    name: 'mv_sales_party_agent_branch',
    relationEnv: 'PIVOT_MV_PARTY_AGENT_BRANCH',
    defaultRelation: 'mv_sales_party_agent_branch',
    dimensions: ['to_party_name', 'agent_name_final', 'branch'],
    filterDimensions: ['to_party_name', 'agent_name_final', 'branch'],
    measureMap: {
      net_amount: 'total_net',
      amount_before_tax: 'total_tax',
      sl_qty: 'total_qty',
    },
  },
  {
    name: 'mv_sales_agent_party_month',
    relationEnv: 'PIVOT_MV_AGENT_PARTY_MONTH',
    defaultRelation: 'mv_sales_agent_party_month',
    dimensions: ['agent_name', 'to_party_name', 'month'],
    filterDimensions: ['agent_name', 'to_party_name', 'month'],
    measureMap: {
      net_amount: 'total_net',
      amount_before_tax: 'total_tax',
      sl_qty: 'total_qty',
    },
    disableGrandTotalBranchExclusion: true,
    monthDimensions: ['month'],
  },
  {
    name: 'sales_mv',
    relationEnv: 'PIVOT_MV_SALES',
    defaultRelation: 'sales_mv',
    dimensions: ['state', 'branch', 'brand', 'month'],
    filterDimensions: ['state', 'branch', 'brand', 'month'],
    measureMap: {
      net_amount: 'total',
      amount_before_tax: 'total_tax',
      sl_qty: 'total_qty',
    },
    monthDimensions: ['month'],
  },
  {
    name: 'sales_pivot_mv',
    relationEnv: 'PIVOT_SOURCE_RELATION',
    defaultRelation: 'mv_sales_all_dims',
    dimensions: [
      'branch', 'fy', 'month', 'mmm', 'region', 'state', 'district', 'city',
      'business_type', 'agent_names_correction', 'party_grouped', 'party_name_for_count',
      'brand', 'agent_name', 'to_party_name',
      'bill_no', 'bill_date',
      'item_no', 'shade_name',
      'rate_unit', 'size', 'units_pack', 'sl_qty',
      'gross_amount', 'amount_before_tax', 'net_amount',
      'sale_order_no', 'sale_order_date',
      'item_with_shade', 'item_category', 'item_sub_cat', 'so_type', 'scheme',
      'goods_type', 'agent_name_final', 'pin_code',
      'created_at',
    ],
    filterDimensions: [
      'branch', 'fy', 'month', 'mmm', 'region', 'state', 'district', 'city',
      'business_type', 'agent_names_correction', 'party_grouped', 'party_name_for_count',
      'brand', 'agent_name', 'to_party_name',
      'bill_no', 'bill_date',
      'item_no', 'shade_name',
      'rate_unit', 'size', 'units_pack', 'sl_qty',
      'gross_amount', 'amount_before_tax', 'net_amount',
      'sale_order_no', 'sale_order_date',
      'item_with_shade', 'item_category', 'item_sub_cat', 'so_type', 'scheme',
      'goods_type', 'agent_name_final', 'pin_code',
      'created_at',
    ],
    measureMap: {
      sl_qty: 'sum_sl_qty',
      gross_amount: 'sum_gross_amount',
      amount_before_tax: 'sum_amount_before_tax',
      net_amount: 'sum_net_amount',
    },
  },
]);

function listFromConfig(config, key) {
  const raw = config?.[key];
  return Array.isArray(raw) ? raw : [];
}

export function resolveMvDimensionField(entry, requestedField) {
  // Some MV dimensions are derivable aliases (e.g. mmm can be read from month).
  if (requestedField === 'mmm' && entry.dimensions?.includes('month') && !entry.dimensions?.includes('mmm')) {
    return 'month';
  }
  return requestedField;
}

function resolveRelation(entry) {
  const raw = String(process.env[entry.relationEnv] || '').trim();
  if (entry.relationEnv === 'PIVOT_MV_SALES' && raw === '0') return '';
  if (raw && raw !== '0') return raw;
  return String(entry.defaultRelation || '').trim();
}

function metricsSupported(entry, values) {
  const map = entry.measureMap || {};
  for (const value of values || []) {
    const agg = String(value?.agg || '').toLowerCase();
    if (agg === 'count') continue;
    if (!['sum', 'avg'].includes(agg)) return false;
    if (!map[value.field]) return false;
  }
  return true;
}

function filtersSupported(entry, sqlFilters) {
  const allowed = new Set(entry.filterDimensions || []);
  for (const f of sqlFilters || []) {
    if (!f?.field) continue;
    if (!allowed.has(f.field)) return false;
  }
  return true;
}

export function getMvRegistry() {
  return DEFAULT_REGISTRY
    .map((entry) => ({
      ...entry,
      relation: resolveRelation(entry),
      dimensions: [...entry.dimensions],
      filterDimensions: [...entry.filterDimensions],
      monthDimensions: [...(entry.monthDimensions || [])],
      measureMap: { ...(entry.measureMap || {}) },
    }))
    .filter((entry) => entry.relation);
}

export function resolveBestMV(normalizedConfig = {}, sqlFilters = []) {
  const requestedDimensionsRaw = [
    ...listFromConfig(normalizedConfig, 'rows'),
    ...listFromConfig(normalizedConfig, 'columns'),
  ];
  const candidates = [];
  for (const entry of getMvRegistry()) {
    const requestedDimensions = requestedDimensionsRaw.map((d) => resolveMvDimensionField(entry, d));
    const requestedSet = new Set(requestedDimensions);
    const mvDims = new Set(entry.dimensions || []);
    const isSubset = [...requestedSet].every((d) => mvDims.has(d));
    if (!isSubset) continue;
    if (!metricsSupported(entry, normalizedConfig.values || [])) continue;
    if (!filtersSupported(entry, sqlFilters)) continue;
    const exact = requestedDimensions.length === entry.dimensions.length;
    const extraDims = entry.dimensions.length - requestedDimensions.length;
    candidates.push({
      ...entry,
      _score: exact ? 10 : 1,
      _extraDims: extraDims,
    });
  }
  candidates.sort((a, b) => {
    if (a._score !== b._score) return b._score - a._score;
    if (a._extraDims !== b._extraDims) return a._extraDims - b._extraDims;
    return a.name.localeCompare(b.name);
  });
  if (!candidates.length) return null;
  const { _score: _s, _extraDims: _e, ...best } = candidates[0];
  return best;
}

export function getPivotCapabilities() {
  const registry = getMvRegistry();
  const dims = new Set();
  const combos = [];
  for (const entry of registry) {
    for (const d of entry.dimensions || []) dims.add(d);
    combos.push(entry.dimensions || []);
  }
  return {
    supportedDimensions: [...dims].sort((a, b) => a.localeCompare(b)),
    mvBackedCombos: combos,
  };
}
