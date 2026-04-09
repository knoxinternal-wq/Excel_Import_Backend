import { listDistinctFyMonthRows } from '../repositories/reportRepository.js';

export async function getReportMetaPayload() {
  const rows = await listDistinctFyMonthRows();
  const fySet = new Set();
  const monthsByFy = new Map();

  for (const r of rows) {
    const fy = String(r.fy || '').trim();
    const month = String(r.month || '').trim();
    if (!fy || !month) continue;
    fySet.add(fy);
    if (!monthsByFy.has(fy)) monthsByFy.set(fy, []);
    monthsByFy.get(fy).push(month);
  }

  const fy = [...fySet].sort((a, b) => a.localeCompare(b));
  const monthsByFyObj = {};
  for (const fyVal of fy) {
    monthsByFyObj[fyVal] = monthsByFy.get(fyVal) || [];
  }
  return { fy, monthsByFy: monthsByFyObj };
}
