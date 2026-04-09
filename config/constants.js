/** Upper bound for sales rows: pagination, pivot scan, import, and UI stay aligned (~18L+ headroom) */
export const MAX_SALES_ROWS = 2_500_000;

/** Skip first 2 rows (Report Name, Generated Date) and ignore column A */
export const SKIP_ROWS = 2;
export const SKIP_COLUMN_A = true;

/** Defer one row while streaming; drop the final data row per sheet (totals/footer row). */
export const SKIP_LAST_DATA_ROW = true;

/** Keywords that identify a grand total row - filter at display time (case-insensitive) */
export const GRAND_TOTAL_ROW_PATTERNS = ['grand total', 'grandtotal', 'total'];

export const REQUIRED_HEADERS = [
  'BRANCH',
  'FY',
  'MONTH',
  'MMM',
  'REGION',
  'STATE',
  'DISTRICT',
  'CITY',
  'TYPE OF Business',
  'Agent Names Correction',
  'PARTY GROUPED',
  'PARTY NAME FOR COUNT',
  'BRAND',
  'AGENT NAME',
  'TO PARTY NAME',
  'BILL NO.',
  'BILL Date',
  'ITEM NAME',
  'SHADE NAME',
  'RATE/UNIT',
  'SIZE',
  'UNITS/PACK',
  'SL QTY',
  'GROSS AMOUNT',
  'AMOUNT BEFORE TAX',
  'NET AMOUNT',
  'SALE ORDER NO.',
  'SALE ORDER Date',
  'Item with Shade',
  'Item Category',
  'Item Sub cat',
  'SO TYPE',
  'SCHEME',
  'GOODS TYPE',
  'AGENT NAME.',
  'PIN CODE',
];

/** Alternate Excel headers → canonical. Keys = normalized form (uppercase, no dots) */
export const HEADER_ALIASES = {
  BRANCH: 'BRANCH',
  'SECOND STATE': 'STATE',
  'CITY NAME': 'CITY',
  LEVEL1: 'DISTRICT',
  SOURCE: 'BRANCH',
  ROUTE: 'BRANCH',
  'BRANCH NAME': 'BRANCH',
  'CITY CODE': 'DISTRICT',
  'CITY NAE': 'CITY',
  'CITY NAME': 'CITY',
  LEVEL: 'DISTRICT',
  Level: 'DISTRICT',
  ZONE: 'REGION',
  'ACC. NAME': 'AGENT NAME',
  'ACCT. NAME': 'AGENT NAME',
  'ACC NAME': 'AGENT NAME',
  'AGENT NAME': 'AGENT NAME', // Excel "AGENT NAME" → agent_name (overrides HEADER_ALIASES_MULTI)
  'AGENT NA': 'AGENT NAME',   // Truncated variants → agent_name
  'AGENT NAM': 'AGENT NAME',
  'AGENTNAMES': 'AGENT NAME', // "AgentNames" (no space)
  'AGENT NAMES': 'AGENT NAME', // "Agent Names" (plural) → agent_name
  'TO DATE NAME': 'TO PARTY NAME',
  'PARTY NAME': 'TO PARTY NAME',
  'BILL NO': 'BILL NO.',
  'BILL DATE': 'BILL Date',
  RATE: 'RATE/UNIT',
  MRP: 'RATE/UNIT',
  'NET AMT': 'NET AMOUNT',
  AMT: 'AMOUNT BEFORE TAX',
  AMOUNT: 'AMOUNT BEFORE TAX',
  QTY: 'SL QTY',
  'JL QTY': 'SL QTY',
  'PL QTY': 'SL QTY',
  PRODUCT: 'Item Category',
  'PRODUCT NAME': 'Item Category',
  'PRODUCT CODE': 'Item Category',
  'PRODUCT CATEGORY': 'Item Category',
  'ITEM CAT': 'Item Category',
  'ITEM CATEGORY': 'Item Category', // explicit (Excel may use exact "Item Category")
  COLLECTION: 'Item Sub cat', // Excel COLLECTION → Item Sub cat (item_sub_cat)
  SCHEME: 'Item Sub cat',     // Excel SCHEME/COLLECTION column (NON COLLECTION, CORE) → Item Sub cat
  'SALE ORDER NO': 'SALE ORDER NO.',
  'SALE ORDER DATE': 'SALE ORDER Date',
  /* Only SALE ORDER NO. variants - do NOT map PARTY ORDER NO or ORDER NO (different columns) */
  'SO NO': 'SALE ORDER NO.',
  'SO NO.': 'SALE ORDER NO.',
  'SALE ORDER': 'SALE ORDER NO.',
  'SALES ORDER NO': 'SALE ORDER NO.',
  'SALES ORDER NO.': 'SALE ORDER NO.',
  'SALE ORDR NO': 'SALE ORDER NO.',
  'SALE ORDR NO.': 'SALE ORDER NO.',
  CATEGORY: 'Item Category',
  'SUB CAT': 'Item Sub cat',
  'ITEM NO': 'ITEM NAME',
  'ITEM NO.': 'ITEM NAME',
  'ITEMNO': 'ITEM NAME',
  'SUB CATEGORY': 'Item Sub cat',
  'TOTAL SELL': 'GROSS AMOUNT',
  'BILL AMOUT': 'GROSS AMOUNT',
  'BILL AMOUNT': 'GROSS AMOUNT',
  'TYPE OF BU': 'TYPE OF Business',
  'TYPE OF Bu': 'TYPE OF Business',
  'TYPE OF BUSINESS': 'TYPE OF Business',
  'TYPE OF Business Unit': 'TYPE OF Business',
  'Agent Names Corr': 'Agent Names Correction',
  'Agent Nam': 'Agent Names Correction',
  /* Removed: 'Agent Name PARTY CRC PARTY NA/NAME' - let HEADER_ALIASES_MULTI map to AGENT NAME (agent_name) */
  'PARTY NAME FOR CNT': 'PARTY NAME FOR COUNT',
  'PARTY GRO': 'PARTY GROUPED',
  'PARTY NAI': 'PARTY NAME FOR COUNT',
  'SHADE MRP RATE/UNIT SIZE': 'RATE/UNIT',
  'SHADE': 'SHADE NAME',
  'UNITS/PAC': 'UNITS/PACK',
  'UNITS / PACK': 'UNITS/PACK',
  'SL. QTY': 'SL QTY',
  'SL QTY.': 'SL QTY',
  'GROSS AMT': 'GROSS AMOUNT',
  'GROSS AMOUNT.': 'GROSS AMOUNT',
  'AMOUNT': 'AMOUNT BEFORE TAX',
  'AMOUNT BEF TAX': 'AMOUNT BEFORE TAX',
  'NET AMOU': 'NET AMOUNT',
  'NET AMT': 'NET AMOUNT',
  'Item Sub Category': 'Item Sub cat',
  'Item Sub Cat': 'Item Sub cat',
  'GOODS TYP': 'GOODS TYPE',
  'AGENT NAME.1': 'AGENT NAME.',
  'SALE ORDER DATE': 'SALE ORDER Date',
  'SALE ORDER Dt': 'SALE ORDER Date',
  'SEL QTY': 'SL QTY',
  'AMOUNT In Rs.': 'AMOUNT BEFORE TAX',
  'Item with S': 'Item with Shade',
  'Item No': 'ITEM NAME',
  'Item No.': 'ITEM NAME',
  'Item Code': 'Item Category',
  'Item Sub C': 'Item Sub cat',
  'TO PARTY 1': 'TO PARTY NAME',
  'AGENT MAP': 'AGENT NAME.',
};

/** One Excel column can satisfy multiple required headers (combined columns) */
export const HEADER_ALIASES_MULTI = {
  'Agent Name PARTY CRC PARTY NA': ['AGENT NAME', 'PARTY GROUPED', 'PARTY NAME FOR COUNT'],
  'Agent Name PARTY CRC PARTY NAME': ['AGENT NAME', 'PARTY GROUPED', 'PARTY NAME FOR COUNT'],
  'Agent Name': ['AGENT NAME'],
  'Party Type': ['PARTY GROUPED'],
  'Party Name': ['PARTY NAME FOR COUNT'],
  'AGENT NAME TO PARTY NA': ['AGENT NAME', 'TO PARTY NAME'],
  'AGENT NAME TO PARTY NAME': ['AGENT NAME', 'TO PARTY NAME'],
  'AGENT NAME TO PARTY 1 BILL NO.': ['AGENT NAME', 'TO PARTY NAME'],
  'ITEM NAME SHADE NAME QTY UNIT SIZE': ['ITEM NAME', 'SHADE NAME', 'SIZE'],
  'QTY UNIT': ['RATE/UNIT'],
  'Item with 1 Item Category': ['Item with Shade', 'Item Category'],
  'Item with Shade 1 Item Category': ['Item with Shade', 'Item Category'],
};

/** Yellow-highlighted columns: do NOT import from file. GOODS TYPE derived from SALE ORDER NO. prefix. Item with Shade derived from ITEM NO + SHADE NAME. */
/** STATE: always imported from Excel column STATE (or "Second State" / header containing "state") → sales_data.state. Do NOT add STATE here. */
/** REGION: do NOT import from Excel; derived from sales_data.state via region_master (state → region) or ref_states/ref_regions. */
export const EXCLUDED_FROM_IMPORT = [
  'FY',
  'MONTH',
  'MMM',
  'REGION',          // Derived from state via region_master / ref_states+ref_regions
  'DISTRICT',        // Never import (wrong data from LEVEL1/CITY CODE aliases); frontend shows '-'
  'TYPE OF Business',
  'Agent Names Correction',
  'SCHEME',           // SCHEME/COLLECTION column data goes to Item Sub cat only (not scheme)
  'GOODS TYPE',       // Calculated from SALE ORDER NO. prefix, not from file
  'AGENT NAME.',
  'Item with Shade',  // Derived from ITEM NO + SHADE NAME (e.g. "6902 5")
  'PIN CODE',         // Derived from party_master_app via TO PARTY NAME -> DISTRICT -> PIN_CODE
];

/** No fields required - import all rows including empty; store null for missing values */
export const REQUIRED_FIELDS = [];
export const NUMERIC_FIELDS = [
  'RATE/UNIT',
  'SL QTY',
  'UNITS/PACK',
  'GROSS AMOUNT',
  'AMOUNT BEFORE TAX',
  'NET AMOUNT',
];

/** `sales_data` column names — single source for pivot, filters, and import math (all brands/branches). */
export const SALES_DATA_NUMERIC_COLUMNS = [
  'rate_unit',
  'sl_qty',
  'units_pack',
  'gross_amount',
  'amount_before_tax',
  'net_amount',
];

export const DATE_FIELDS = ['BILL Date', 'SALE ORDER Date'];
/** Initial rows per insert batch (adaptive between MIN/MAX per worker round-trip time). */
export const BATCH_SIZE = 2500;
export const BATCH_SIZE_MIN = 800;
export const BATCH_SIZE_MAX = 5000;
/** Parallel insert workers (override with IMPORT_BATCH_CONCURRENCY). */
export const BATCH_CONCURRENCY = 6;
export const JOB_UPDATE_EVERY_N_BATCHES = 24;  // Throttle import_jobs writes on large files
export const ERROR_BATCH_SIZE = 50;  // Buffer failed rows before bulk insert
