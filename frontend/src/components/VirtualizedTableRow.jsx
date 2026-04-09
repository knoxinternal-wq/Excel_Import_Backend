import { memo } from 'react';
import { getSalesCellDisplayValue, NUMERIC_KEYS } from '../utils/salesTableCellFormat';

function VirtualizedTableRow({ virtualRow, row, columns, totalWidth }) {
  if (!row) return null;
  return (
    <div
      style={{
        position: 'absolute',
        top: 0,
        left: 0,
        width: totalWidth,
        boxSizing: 'border-box',
        height: `${virtualRow.size}px`,
        transform: `translateY(${Math.round(virtualRow.start)}px)`,
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
}

function propsEqual(prev, next) {
  if (prev.virtualRow.index !== next.virtualRow.index) return false;
  if (prev.virtualRow.start !== next.virtualRow.start) return false;
  if (prev.virtualRow.size !== next.virtualRow.size) return false;
  if (prev.row !== next.row) return false;
  if (prev.totalWidth !== next.totalWidth) return false;
  return prev.columns === next.columns;
}

export default memo(VirtualizedTableRow, propsEqual);
