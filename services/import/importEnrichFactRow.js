/**
 * Node-side enrichment for sales_data rows (replaces SQL joins + CASE in transformStagingToFact).
 */
import { normalizePartyName, getPartyNameAliasKeys, normalizeAgentName, getAgentNameExactKey } from '../../utils/normalizeHeader.js';
import { resolveBusinessType } from '../../utils/customerTypeMaster.js';
import { resolveSoType } from '../../utils/soTypeMaster.js';

const MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/**
 * Indian FY from bill_date (Apr–Mar), same label as deriveFYMonthFromBillDate / admin FY (e.g. 2025-26).
 * @param {string|Date|null|undefined} date
 * @returns {string|null}
 */
export function getFY(date) {
  if (!date) return null;
  let d;
  if (date instanceof Date) {
    d = date;
  } else {
    const s = String(date).trim();
    const ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (ymd) d = new Date(parseInt(ymd[1], 10), parseInt(ymd[2], 10) - 1, parseInt(ymd[3], 10));
    else d = new Date(s);
  }
  if (!d || isNaN(d.getTime()) || d.getFullYear() < 2000) return null;
  const y = d.getFullYear();
  const m = d.getMonth() + 1;
  const fyYear = m >= 4 ? y : y - 1;
  return `${fyYear}-${String(fyYear + 1).slice(-2)}`;
}

function toNum(v) {
  if (v == null || v === '') return null;
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  const n = parseFloat(String(v).replace(/,/g, ''));
  return Number.isFinite(n) ? n : null;
}

/** Indian FY + month labels from bill_date (same semantics as SQL / legacy deriveFYMonthFromBillDate). */
function deriveFYMonthFromBillDate(data) {
  const billDate = data.bill_date;
  if (!billDate) return;
  let d;
  if (billDate instanceof Date) {
    d = billDate;
  } else {
    const s = String(billDate).trim();
    const ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (ymd) d = new Date(parseInt(ymd[1], 10), parseInt(ymd[2], 10) - 1, parseInt(ymd[3], 10));
    else d = new Date(s);
  }
  if (!d || isNaN(d.getTime())) return;
  const year = d.getFullYear();
  const monthNum = d.getMonth() + 1;
  const fyYear = monthNum >= 4 ? year : year - 1;
  const mmmLabel = MONTH_NAMES[monthNum - 1] || '';
  if (!data.fy || String(data.fy).trim() === '') {
    data.fy = `${fyYear}-${String(fyYear + 1).slice(-2)}`;
  }
  if (!data.month || String(data.month).trim() === '') {
    data.month = mmmLabel ? `${mmmLabel}-${String(year).slice(-2)}` : null;
  }
  if (!data.mmm || String(data.mmm).trim() === '') {
    data.mmm = mmmLabel ? mmmLabel.toUpperCase() : null;
  }
}

function applyRareWoolBranchRule(data) {
  const brand = data?.brand != null ? String(data.brand).trim().toUpperCase() : '';
  const branch = data?.branch != null ? String(data.branch).trim().toUpperCase() : '';
  if (brand === 'RARE WOOL' && branch === 'DON AND JULIO') data.branch = 'RARE WOOL';
}

function canonicalizeBrandForSoMaster(rawBrand) {
  const b = rawBrand != null ? String(rawBrand).trim().toUpperCase() : '';
  if (!b) return '';

  // Don & Julio family should map to "DON AND JULIO" in master keys.
  if ([
    'RARE WOOL',
    'DON AND JULIO',
    'DON & JULIO',
    'DON&JULIO',
    'DON &JULIO',
    'D&J',
    'DNJ',
  ].includes(b)) {
    return 'DON AND JULIO';
  }

  if (b === 'ITALIAN CHANNEL' || b === 'IC') return 'ITALIAN CHANNEL';
  if (b === 'RISHAB FABRICS' || b === 'RF' || b === 'RISHAB') return 'RISHAB FABRICS';
  if (b === 'VERCELLI') return 'VERCELLI';
  return b;
}

function deriveGoodsType(saleOrderNo) {
  if (saleOrderNo == null || saleOrderNo === '') return null;
  let so = String(saleOrderNo).trim();
  if (!so) return null;
  so = so.replace(/^['"]+/, '').trim().toUpperCase();
  if (!so) return null;
  const prefixMatch = so.match(/^([A-Z]+)/);
  const prefix = prefixMatch ? prefixMatch[1] : '';
  if (prefix === 'MID') return 'MID';
  if (prefix === 'MIX') return 'MIX';
  if (prefix === 'SRC') return 'SRC';
  if (prefix === 'ACS' || prefix === 'ACC') return 'ACC';
  return 'Fresh';
}

function deriveRegionFromState(data, regionMap) {
  const stateKey = data?.state != null ? String(data.state).trim().toUpperCase() : '';
  if (!stateKey) return;
  const mapped = regionMap?.get(stateKey);
  if (mapped) data.region = mapped;
}

function deriveDistrictAndPinCodeFromToPartyName(data, partyMasterMap) {
  const toPartyName = data?.to_party_name;
  if (toPartyName == null || String(toPartyName).trim() === '') return;
  const key = normalizePartyName(toPartyName);
  let match = partyMasterMap?.get(key);
  if (!match) {
    for (const alt of getPartyNameAliasKeys(toPartyName)) {
      match = partyMasterMap?.get(alt);
      if (match) break;
    }
  }
  if (match?.district) data.district = match.district;
  if (match?.pin_code) data.pin_code = match.pin_code;
}

function derivePartyGrouping(data, partyGroupingMap) {
  const toPartyName = data.to_party_name;
  const excelPartyGrouped = data.party_grouped;
  const excelPartyNameForCount = data.party_name_for_count;
  const toPartyTrimmed = toPartyName != null && String(toPartyName).trim() !== '' ? String(toPartyName).trim() : null;

  if (!toPartyTrimmed) {
    data.party_grouped = excelPartyGrouped ?? null;
    data.party_name_for_count = excelPartyNameForCount ?? null;
    return;
  }

  const key = normalizePartyName(toPartyName);
  let master = partyGroupingMap?.get(key);
  if (!master) {
    for (const alt of getPartyNameAliasKeys(toPartyName)) {
      master = partyGroupingMap?.get(alt);
      if (master) break;
    }
  }

  if (master) {
    data.party_name_for_count = master.party_name_for_count ?? toPartyTrimmed;
    data.party_grouped = master.party_grouped ?? toPartyTrimmed;
  } else {
    data.party_name_for_count = excelPartyNameForCount ?? toPartyTrimmed;
    // Business rule: if no party_grouping_master match, party_grouped should mirror TO PARTY NAME.
    data.party_grouped = toPartyTrimmed;
  }
}

function deriveAgentNameFields(data, agentNameMap) {
  const agentName = data.agent_name;
  const excelNameRaw = agentName != null ? String(agentName) : '';
  const excelNameTrimmed = excelNameRaw.trim();
  const excelName = excelNameTrimmed || null;
  const exactKey = getAgentNameExactKey(agentName);
  const normalizedKey = normalizeAgentName(agentName);
  const combinedName = (exactKey || normalizedKey)
    ? (agentNameMap?.get(exactKey) ?? agentNameMap?.get(normalizedKey) ?? null)
    : null;
  const displayName = combinedName || excelName;
  data.agent_name_final = displayName;
  data.agent_names_correction = displayName;
}

function deriveItemWithShade(data) {
  const itemNo = data.item_no != null ? String(data.item_no).trim() : '';
  const shadeName = data.shade_name != null ? String(data.shade_name).trim() : '';
  const parts = [itemNo, shadeName].filter((s) => s !== '');
  data.item_with_shade = parts.length > 0 ? parts.join(' ') : null;
}

/**
 * TYPE OF Business (`business_type`):
 * - Match `sales_data.to_party_name` to `customer_type_master.party_name` (via normalized map built from that table).
 * - Use `customer_type_master.type` (column may appear as `type` / `TYPE` from PostgREST).
 * - If no row matches, use RETAILER only (see `resolveBusinessType`).
 * - Excel "TYPE OF Business" is not imported; this is the sole source during ingest.
 * - ITALIAN CHANNEL + RETAILER → DISTRIBUTOR (existing business rule).
 */
function applyBusinessTypeAndItalianChannel(data, customerTypeByParty) {
  const resolved = resolveBusinessType(data.to_party_name, customerTypeByParty);
  const branchForRule = data?.branch != null ? String(data.branch).trim().toUpperCase() : '';
  const typeForRule = String(resolved || '').trim().toUpperCase();
  if (branchForRule === 'ITALIAN CHANNEL' && typeForRule === 'RETAILER') {
    data.business_type = 'DISTRIBUTOR';
  } else {
    data.business_type = resolved;
  }
}

/**
 * Mutates `data` in place (fact-shaped row). `data` may contain string fields from CSV.
 */
export function enrichImportFactRow(data, masters) {
  const {
    customerTypeByParty,
    soTypeByParty,
    partyGroupingMap,
    agentNameMap,
    regionMap,
    partyMasterMap,
    soMasterMap,
  } = masters;

  applyRareWoolBranchRule(data);
  deriveFYMonthFromBillDate(data);
  // Normalize sales BRAND into the canonical values used by the admin-uploaded masters.
  data.brand = canonicalizeBrandForSoMaster(data.brand);
  const fyFromBill = getFY(data.bill_date);
  if ((!data.fy || String(data.fy).trim() === '') && fyFromBill) {
    data.fy = fyFromBill;
  }
  deriveRegionFromState(data, regionMap);
  deriveDistrictAndPinCodeFromToPartyName(data, partyMasterMap);
  derivePartyGrouping(data, partyGroupingMap);
  deriveAgentNameFields(data, agentNameMap);
  deriveItemWithShade(data);
  applyBusinessTypeAndItalianChannel(data, customerTypeByParty);

  data.goods_type = deriveGoodsType(data.sale_order_no);

  const slQty = toNum(data.sl_qty);
  if (slQty != null && slQty <= 0) {
    data.so_type = 'RETURN';
  } else {
    // Business override: for RISHAB FABRICS, non-return rows should default to NO SCHEME.
    const normalizedBrand = data.brand != null ? String(data.brand).trim().toUpperCase() : '';
    if (normalizedBrand === 'RISHAB FABRICS') {
      data.so_type = 'NO SCHEME';
      return;
    }

    const party =
      (data.to_party_name ?? data.party_name) != null
        ? String(data.to_party_name ?? data.party_name).trim().toUpperCase()
        : '';
    const brand = data.brand != null ? String(data.brand).trim().toUpperCase() : '';
    const fy = data.fy != null ? String(data.fy).trim() : '';
    const key = `${party}|${brand}|${fy}`;
    const mapped = soMasterMap?.get(key);
    if (mapped != null && mapped !== '') {
      data.so_type = mapped;
    } else {
      const resolved = resolveSoType(data.to_party_name, soTypeByParty, data.so_type ?? null);
      data.so_type = resolved ?? data.so_type ?? null;
      if (data.so_type == null || String(data.so_type).trim() === '') {
        data.so_type = 'UNKNOWN';
      }
    }
  }

  delete data.__import_rn;
  return data;
}
