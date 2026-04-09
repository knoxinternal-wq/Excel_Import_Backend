/**
 * Normalize Excel header for case-insensitive matching.
 * BRANCH, Branch, branch, BRANCH. all resolve to "BRANCH"
 */
export function normalizeHeader(header) {
  if (!header) return '';

  return header
    .toString()
    .replace(/\u00A0/g, ' ')  // Excel non-breaking space → space
    .trim()
    .replace(/\./g, '')
    .replace(/\s+/g, ' ')
    .toUpperCase();
}

/**
 * Normalize party name for matching TO PARTY NAME to party_grouping_master.party_name.
 * Handles Excel quirks: non-breaking spaces, unicode dashes, multiple spaces, trim, case-insensitive.
 * Use consistently when building map keys AND when looking up.
 */
export function normalizePartyName(name) {
  if (name == null || name === '') return '';
  return String(name)
    .replace(/\u00A0/g, ' ')       // non-breaking space → space
    .replace(/[\u2013\u2014\u2015\u2212]/g, '-')  // en-dash, em-dash → hyphen
    .replace(/\s*-\s*/g, ' - ')    // normalize hyphen spacing: "A-B", "A - B", "A- B" → "A - B"
    .replace(/[\t\r\n]+/g, ' ')    // tabs, CR, LF → space
    .replace(/\s+/g, ' ')          // collapse multiple whitespace to single space
    .trim()
    .toLowerCase();
}

const UNICODE_SPACE_CHARS = /[\u00A0\u1680\u2000-\u200A\u202F\u205F\u3000]/g;

/**
 * Match key for **both** `customer_type_master.party_name` **and** `sales_data.to_party_name`.
 * Build the map with this on master rows; look up with this on sales rows — same function, same key.
 *
 * Preserves internal spaces and hyphen-adjacent spacing (e.g. `COMPANY -RAXAUL` stays distinct from `COMPANY - RAXAUL`).
 * NFKC, unicode spaces → ASCII space, unicode dashes → '-', tab/newline → single space (no run collapse),
 * trim ends only, uppercase for case-insensitive comparison.
 */
export function normalizePartyNameForCustomerTypeExact(name) {
  if (name == null || name === '') return '';
  let s = String(name).normalize('NFKC');
  s = s.replace(UNICODE_SPACE_CHARS, ' ');
  s = s.replace(/[\u2013\u2014\u2015\u2212]/g, '-');
  s = s.replace(/\t/g, ' ');
  s = s.replace(/\r\n|\r|\n/g, ' ');
  s = s.trim();
  return s.toUpperCase();
}

/**
 * Return alias keys for party lookup.
 * E.g. master has "M/S ABC TRADERS" → also match when Excel has "ABC TRADERS".
 */
export function getPartyNameAliasKeys(name) {
  if (!name || typeof name !== 'string') return [];
  const s = String(name).replace(/\u00A0/g, ' ').replace(/\s+/g, ' ').trim();
  if (!s) return [];
  const keys = [];
  const stripped = s.replace(/^m\/s\.?\s*/i, '').replace(/^ms\.?\s*/i, '').replace(/^m\/s\s*/i, '').trim();
  if (stripped && normalizePartyName(stripped) !== normalizePartyName(s)) {
    keys.push(normalizePartyName(stripped));
  }
  return keys;
}

/**
 * Return raw to_party_name values that would match this master party_name (for .in() filter).
 * Includes party_name and stripped form (without M/S) so both "M/S ABC TRADERS" and "ABC TRADERS" match.
 */
export function getPartyNameFilterValues(partyName) {
  if (!partyName || typeof partyName !== 'string') return [];
  const s = String(partyName).replace(/\u00A0/g, ' ').replace(/\s+/g, ' ').trim();
  if (!s) return [];
  const values = [s];
  const stripped = s.replace(/^m\/s\.?\s*/i, '').replace(/^ms\.?\s*/i, '').replace(/^m\/s\s*/i, '').trim();
  if (stripped && stripped !== s) values.push(stripped);
  return values;
}

/**
 * Normalize agent names for case-insensitive matching with agent_name_master.
 */
export function normalizeAgentName(name) {
  if (name == null || name === '') return '';
  return String(name)
    .replace(/\u00A0/g, ' ')
    .replace(/\./g, '')
    .replace(/[\u2013\u2014\u2015\u2212]/g, '-')
    .replace(/\s*-\s*/g, ' - ')
    .replace(/[\t\r\n]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}

/**
 * Exact-ish key used for direct case-insensitive matching before broader normalization.
 */
export function getAgentNameExactKey(name) {
  if (name == null || name === '') return '';
  return String(name)
    .replace(/\u00A0/g, ' ')
    .replace(/[\t\r\n]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();
}
