import React, { memo } from 'react';
import { getSalesCellDisplayValue, NUMERIC_KEYS } from '../utils/salesTableCellFormat';

export const VirtualizedTableRow = memo(function VirtualizedTableRow({
  row,
  style,
  virtualRow,
  columns,
  totalWidth,
}) {
  if (!row) return null;
  const top = style?.top ?? Math.round(virtualRow?.start ?? 0);
  const rowHeight = style?.height ?? `${virtualRow?.size}px`;
  return (
    <div
      style={{
        position: 'absolute',
        top: 0,
        left: 0,
        width: totalWidth,
        boxSizing: 'border-box',
        height: typeof rowHeight === 'number' ? `${rowHeight}px` : rowHeight,
        transform: `translateY(${top}px)`,
      }}
      className="flex items-center border-b border-slate-100 hover:bg-slate-50/80"
    >
      {columns.map((col) => {
        const val = getSalesCellDisplayValue(row, col);
        const isNumeric = NUMERIC_KEYS.includes(col.key);
        return (
          <div
            key={col.key}
            style={{ width: col.width, minWidth: col.width, boxSizing: 'border-box' }}
            className={`flex-shrink-0 truncate px-3 py-2 text-[13px] text-slate-700 ${
              isNumeric ? 'text-right tabular-nums font-medium' : ''
            }`}
          >
            {val ?? '-'}
          </div>
        );
      })}
    </div>
  );
}, (prev, next) =>
  prev.row?.id === next.row?.id
  && (prev.style?.top ?? prev.virtualRow?.start) === (next.style?.top ?? next.virtualRow?.start));

export default VirtualizedTableRow;
