/**
 * Build the same row / subtotal sequence as the pivot UI (matches PivotReport orderedBodyRows).
 * Used only for optional response body windowing after full aggregation.
 */
export function buildOrderedPivotBodyLines(rowHeaders, rowSubtotals, rows, subtotalFields) {
  const safeHeaders = Array.isArray(rowHeaders) ? rowHeaders : [];
  if (!safeHeaders.length) return [];

  const depths = (Array.isArray(subtotalFields) ? subtotalFields : [])
    .map((f) => rows.indexOf(f) + 1)
    .filter((d) => d > 0);
  const subtotalDepthSet = new Set(depths);
  const visibleSubtotals = (Array.isArray(rowSubtotals) ? rowSubtotals : []).filter(
    (st) => subtotalDepthSet.has(st.depth),
  );

  if (!visibleSubtotals.length || subtotalDepthSet.size === 0) {
    return safeHeaders.map((rh) => ({ type: 'row', row: rh }));
  }

  const subtotalByKey = new Map(visibleSubtotals.map((st) => [st.key, st]));
  const selectedDepthsDesc = [...subtotalDepthSet].sort((a, b) => b - a);
  const out = [];

  const prefixKey = (labels, depth) => [...labels.slice(0, depth), '__subtotal__'].join('||');
  const prefix = (labels, depth) => labels.slice(0, depth).join('||');

  for (let i = 0; i < safeHeaders.length; i += 1) {
    const rh = safeHeaders[i];
    const next = safeHeaders[i + 1];
    out.push({ type: 'row', row: rh });

    for (const depth of selectedDepthsDesc) {
      if (!rh.labels || rh.labels.length < depth) continue;
      const curPrefix = prefix(rh.labels, depth);
      const nextPrefix = next?.labels ? prefix(next.labels, depth) : null;
      if (curPrefix !== nextPrefix) {
        const st = subtotalByKey.get(prefixKey(rh.labels, depth));
        if (st) out.push({ type: 'subtotal', subtotal: st });
      }
    }
  }
  return out;
}

/**
 * @param {object} result - Full pivot result from runPivot (before JSON serialization)
 * @param {{ rows: string[], subtotalFields: string[], bodyOffset: number, bodyLimit: number }} win
 * @returns {{ result: object, bodyPaging: { totalLines: number, offset: number, limit: number, truncatedAfter: boolean } }}
 */
export function applyPivotBodyWindow(result, win) {
  const { rows, subtotalFields, bodyOffset, bodyLimit } = win;
  if (bodyLimit == null || !Number.isFinite(bodyLimit) || bodyLimit <= 0) {
    return { result, bodyPaging: null };
  }

  const lines = buildOrderedPivotBodyLines(
    result.rowHeaders,
    result.rowSubtotals,
    rows,
    subtotalFields,
  );
  const totalLines = lines.length;
  const sliceLines = lines.slice(bodyOffset, bodyOffset + bodyLimit);
  const truncatedAfter = bodyOffset + sliceLines.length < totalLines;

  const rowKeys = new Set();
  for (const line of sliceLines) {
    if (line.type === 'row' && line.row?.key != null) rowKeys.add(line.row.key);
  }

  const nextCells = {};
  const nextRowTotals = {};
  for (const k of rowKeys) {
    if (result.cells[k]) nextCells[k] = result.cells[k];
    if (result.rowTotals[k]) nextRowTotals[k] = result.rowTotals[k];
  }

  const subKeys = new Set();
  for (const line of sliceLines) {
    if (line.type === 'subtotal' && line.subtotal?.key != null) subKeys.add(line.subtotal.key);
  }
  const nextRowSubtotals = (result.rowSubtotals || []).filter((st) => subKeys.has(st.key));

  const nextRowHeaders = sliceLines.filter((l) => l.type === 'row').map((l) => l.row);

  const bodyPaging = {
    totalLines,
    offset: bodyOffset,
    limit: bodyLimit,
    truncatedAfter,
  };

  return {
    result: {
      ...result,
      rowHeaders: nextRowHeaders,
      cells: nextCells,
      rowTotals: nextRowTotals,
      rowSubtotals: nextRowSubtotals,
      bodyLines: sliceLines,
      meta: {
        ...result.meta,
        bodyPaging,
      },
    },
    bodyPaging,
  };
}
