import { useMemo, useState, memo } from 'react';
import {
  FileSpreadsheet,
  Upload,
  Database,
  BarChart3,
  Shield,
  LogOut,
  ChevronLeft,
  ChevronRight,
} from 'lucide-react';

const SIDEBAR_EXPANDED = 'w-[260px]';
const SIDEBAR_COLLAPSED = 'w-[88px]';

export const MenuItem = memo(function MenuItem({
  icon: Icon,
  label,
  isActive = false,
  collapsed = false,
  onClick,
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        'group relative w-full flex items-center rounded-xl px-2.5 py-2.5 transition-colors duration-200',
        'focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/50',
        isActive
          ? 'bg-blue-500/15 text-blue-100 ring-1 ring-blue-500/35 shadow-[0_0_0_1px_rgba(59,130,246,0.2),0_0_22px_-8px_rgba(59,130,246,0.75)]'
          : 'text-slate-300 hover:bg-white/5 hover:text-slate-100',
        collapsed ? 'justify-center' : 'justify-start gap-3',
      ].join(' ')}
      aria-label={label}
      aria-current={isActive ? 'page' : undefined}
    >
      <span
        className={[
          'inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border transition-colors duration-200',
          isActive
            ? 'border-blue-400/40 bg-blue-500/20 text-blue-200'
            : 'border-white/10 bg-white/5 text-slate-400 group-hover:text-slate-200',
        ].join(' ')}
        aria-hidden
      >
        <Icon className="h-4 w-4" />
      </span>

      {!collapsed && <span className="truncate text-sm font-medium">{label}</span>}

      {collapsed && (
        <span
          className="pointer-events-none absolute left-[84px] top-1/2 z-30 -translate-y-1/2 whitespace-nowrap rounded-md border border-white/10 bg-slate-900 px-2 py-1 text-xs text-slate-100 opacity-0 shadow-lg transition-opacity duration-200 group-hover:opacity-100"
          role="tooltip"
        >
          {label}
        </span>
      )}
    </button>
  );
});

export default function Sidebar({
  activeTab = 'report',
  onSelectTab,
  userEmail = 'vishal@rishabworld.com',
  onLogout,
  collapsed,
  onCollapsedChange,
}) {
  const [internalCollapsed, setInternalCollapsed] = useState(false);
  const isControlled = typeof collapsed === 'boolean';
  const isCollapsed = isControlled ? collapsed : internalCollapsed;

  const menu = useMemo(
    () => [
      { id: 'import', label: 'Import', icon: Upload },
      { id: 'data', label: 'Data', icon: Database },
      { id: 'report', label: 'Report', icon: BarChart3 },
      { id: 'admin', label: 'Admin', icon: Shield },
    ],
    [],
  );

  return (
    <aside
      className={[
        'fixed left-0 top-0 z-40 hidden h-screen shrink-0 border-r border-white/10',
        'bg-gradient-to-b from-[#0B1220] to-[#0F172A]',
        'shadow-[6px_0_30px_-12px_rgba(2,6,23,0.8)] backdrop-blur-xl md:flex',
        'transition-[width] duration-200 ease-out',
        isCollapsed ? SIDEBAR_COLLAPSED : SIDEBAR_EXPANDED,
      ].join(' ')}
      aria-label="Sidebar navigation"
    >
      <div className={['flex h-full w-full flex-col py-4', isCollapsed ? 'px-2' : 'px-3'].join(' ')}>
        {/* Top: Workspace */}
        <div className={isCollapsed ? 'px-0.5' : 'px-1'}>
          <div className={['flex items-center', isCollapsed ? 'justify-center' : 'gap-3'].join(' ')}>
            <div className={['inline-flex shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-blue-500 to-blue-700 text-white shadow-[0_8px_24px_-12px_rgba(59,130,246,0.9)]', isCollapsed ? 'h-11 w-11' : 'h-10 w-10'].join(' ')}>
              <FileSpreadsheet className="h-5 w-5" />
            </div>
            {!isCollapsed && (
              <div className="min-w-0">
                <p className="truncate text-sm font-semibold text-white">Sales Report</p>
                <p className="mt-0.5 text-[10px] uppercase tracking-[0.2em] text-slate-400">Workspace</p>
              </div>
            )}
          </div>
        </div>

        {/* Modules */}
        <nav
          className={[
            'flex-1 overflow-y-auto overflow-x-hidden [scrollbar-width:none] [&::-webkit-scrollbar]:hidden',
            isCollapsed ? 'mt-4 px-0.5' : 'mt-5 px-1',
          ].join(' ')}
          aria-label="Main navigation"
        >
          {!isCollapsed && (
            <p className="px-2 pb-2 text-[10px] font-semibold uppercase tracking-[0.2em] text-slate-500/90">
              Modules
            </p>
          )}
          <ul className={isCollapsed ? 'space-y-2.5' : 'space-y-1.5'}>
            {menu.map((item) => (
              <li key={item.id}>
                <MenuItem
                  icon={item.icon}
                  label={item.label}
                  isActive={activeTab === item.id}
                  collapsed={isCollapsed}
                  onClick={() => onSelectTab?.(item.id)}
                />
              </li>
            ))}
          </ul>
        </nav>

        {/* Bottom: User profile */}
        <div className={['px-1', isCollapsed ? 'mt-2.5' : 'mt-3'].join(' ')}>
          {!isCollapsed && (
            <div className="px-2.5 py-2">
              <p className="text-[10px] uppercase tracking-[0.2em] text-slate-500">Signed in</p>
              <p className="mt-1 truncate text-sm text-slate-200" title={userEmail}>
                {userEmail}
              </p>
            </div>
          )}

          <div className={['border-t border-white/10', isCollapsed ? 'mt-2 pt-2.5 flex justify-center' : 'mt-2 pt-2'].join(' ')}>
            <MenuItem
              icon={LogOut}
              label="Logout"
              collapsed={isCollapsed}
              onClick={onLogout}
            />
          </div>
        </div>

        {/* Bonus: Collapse toggle */}
        <button
          type="button"
          onClick={() => {
            const next = !isCollapsed;
            if (!isControlled) setInternalCollapsed(next);
            onCollapsedChange?.(next);
          }}
          className={[
            'inline-flex h-9 items-center rounded-xl border border-white/10 bg-white/[0.02] text-slate-300 transition-colors duration-200 hover:bg-white/[0.06] hover:text-white',
            isCollapsed ? 'mt-2.5 justify-center px-0' : 'mt-3 justify-center gap-2 px-2.5',
          ].join(' ')}
          aria-label={isCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        >
          {isCollapsed ? <ChevronRight className="h-4 w-4" /> : <ChevronLeft className="h-4 w-4" />}
          {!isCollapsed && <span className="text-xs font-medium">Collapse</span>}
        </button>
      </div>
    </aside>
  );
}
