# Data Tab and Table Loading Guide

This document explains how the **Data** tab works end-to-end in this project, from UI rendering to backend query execution and response shaping.

## 1) Purpose of the Data Tab

The Data tab is the operational view for loaded `sales_data` rows.  
It is designed for:

- Fast page-based browsing of large datasets
- Stable UI performance using virtualization
- Safe periodic refresh without blocking user actions
- Predictable payload size (only selected columns)

The Data tab does **not** load all rows at once. It always requests one page from the backend.

## 2) Frontend Entry Point

Main file: `frontend/src/pages/Dashboard.jsx`

Behavior:

- Active tabs are `import`, `data`, `report`.
- `data` tab renders `VirtualizedTable`.
- After import completion, Dashboard prefetches page 1 and passes it to table via `bootstrap` prop.
- Then it switches the active tab to `data` and triggers a refresh.

Relevant flow:

1. Import completes
2. Dashboard calls `dataApi.fetch({ page: 1, limit: 100, includeTotal: 1, sortBy: 'id', sortOrder: 'desc' })`
3. Result passed as `bootstrap` into `VirtualizedTable`
4. Table paints immediately using bootstrap data, then performs silent revalidation

## 3) Data Table Component

Main file: `frontend/src/components/VirtualizedTable.jsx`

### Core responsibilities

- Request paged rows from backend
- Keep pagination state (`page`, `limit`, `total`, `totalPages`)
- Render only visible rows using `@tanstack/react-virtual`
- Show loader/error/empty states
- Auto-refresh current page at interval

### Columns and schema mapping

- `COLUMNS` list defines table headers, keys, and widths.
- Keys map directly to backend row fields from `sales_data` + enriched fields.
- Total width is precomputed (`TOTAL_WIDTH`) for horizontal scroll.

### Virtualization strategy

- Row height is fixed (`ROW_HEIGHT = 40`).
- Header is sticky.
- Only visible rows + overscan are mounted.
- This prevents heavy DOM rendering for 100+ row pages with many columns.

## 4) Frontend Loading Lifecycle

### Initial load

- If `bootstrap` exists: table starts with ready data and does silent fetch for freshness.
- If no bootstrap: table shows loading screen and fetches page 1.

### Page changes

- Prev/Next controls call `goToPage`.
- `fetchData(page, false)` loads requested page.
- If a request is in-flight, previous request is aborted using `AbortController`.

### Page size change

- Rows per page options: `25, 50, 100, 200` (backend supports 10–300).
- Changing size resets page to 1 and fetches again.

### Refresh behavior

- Controlled by `refreshInterval` prop from Dashboard:
  - While import running: fast refresh (`800ms`)
  - Normal idle mode: `8000ms`
- When browser tab is hidden, refresh slows down.
- On tab visibility restore, it refreshes immediately once.

### Error/cancel handling

- Abort errors are ignored (not shown to user).
- Real API errors are shown with Retry action.

## 5) API Client Layer

Main file: `frontend/src/services/api.js`

- Data endpoint client:
  - `dataApi.fetch(params, config) => GET /api/data`
  - `dataApi.getStates() => GET /api/data/states`

Used by:

- `VirtualizedTable` for rows
- filter/state UI areas (where needed)

## 6) Backend Route and Controller

Route file: `backend/routes/data.js`  
Controller: `backend/controllers/dataController.js`

### Endpoints related to Data tab

- `GET /api/data` -> main paged dataset
- `GET /api/data/states` -> distinct states for filters

Report endpoints in same route file are for Pivot/Report tab, not Data table rendering.

## 7) `/api/data` Request Handling (Backend)

`getData(req, res)` performs these steps:

1. Parse query params:
   - `page`, `limit`, `includeTotal`, `search`, `state`, `sortBy`, `sortOrder`
2. Validate/sanitize:
   - limit clamped to `10..300`
   - sort column restricted to allowlist
3. Build base select (`DATA_SELECT`) with only needed fields (no `SELECT *`)
4. Apply filters:
   - state equality filter
   - search on `bill_no` + `party_grouped` (and optional party master expansion)
5. Optional exact count:
   - if `includeTotal=1`, runs exact count query
   - uses cache (`getOrLoadMaster`) with TTL to avoid repeated expensive `COUNT(*)`
6. Compute `from/to` range for page
7. Execute main query:
   - ordered and ranged (`.order(...).range(from, to)`)
8. Enrich rows in one pass (`enrichRowsSinglePass`)
9. Return:
   - `{ data: enrichedRows, pagination: { page, limit, total, totalPages } }`

## 8) Backend Enrichment Layer

The API enriches raw rows before returning to frontend:

- `business_type` from customer master
- `so_type` derivation
- region mapping from state
- district/pin from party master
- party grouping/name-for-count mapping
- agent combined name mapping
- FY/month/MMM derivation fallback
- `item_with_shade` derived value
- branch override rule for Rare Wool
- grand total row suppression

This means Data tab receives display-ready rows and keeps frontend logic light.

## 9) Pagination Contract

Response pagination object:

- `page`: current resolved page
- `limit`: rows per page
- `total`: exact total when available, otherwise fallback estimate
- `totalPages`: computed from total or inferred fallback

Frontend trusts this contract and clamps UI navigation accordingly.

## 10) Performance Design Decisions

### Why Data tab remains responsive

- Server-side pagination only
- Bounded page size (max 300)
- Column-limited select list
- Request cancellation on page changes
- Virtualized rendering
- Background refresh instead of full reload

### Why backend remains efficient

- Count query is cached
- Query result is ranged and sorted at DB level
- Search expansion is bounded (`MAX_PARTY_NAME_MATCHES`)
- Enrichment runs in a single pass after fetch

## 11) States Endpoint (`/api/data/states`)

`getStates` logic:

1. Try view `v_distinct_states` first (fast path)
2. Fallback: paged scan of `sales_data` state column
3. Deduplicate + sort and return array

This supports dropdown/filter UIs without heavy full-table response.

## 12) Data Tab During Active Import

While import is ongoing:

- Dashboard gives table shorter refresh interval (800ms).
- Data tab continues loading only current page.
- UI does not block because import and data fetch are separate API calls.

## 13) Known Limits and Constraints

- Backend enforces `limit <= 300`.
- Sortable columns must be in allowlist.
- Search is optimized for practical patterns, not arbitrary full-text analytics.
- `MAX_SALES_ROWS` constraints still apply where configured.

## 14) Quick Sequence Diagram (Text)

1. User opens Data tab  
2. `VirtualizedTable` calls `GET /api/data?page=1&limit=100...`  
3. Backend runs filtered paged query + enrichment  
4. Backend returns rows + pagination  
5. Table renders header + virtualized rows  
6. Timer/visibility logic triggers silent refreshes for current page

## 15) Key Files Reference

- `frontend/src/pages/Dashboard.jsx`
- `frontend/src/components/VirtualizedTable.jsx`
- `frontend/src/services/api.js`
- `backend/routes/data.js`
- `backend/controllers/dataController.js`
- `backend/services/masterLoaders.js` (master maps used by enrichment)
- `backend/services/masterLookupCache.js` (count/master cache utility)

## 16) How Filters Work (Data Tab)

### Filter inputs and request flow

When user applies filters/search in Data tab:

1. Frontend sends query params to `GET /api/data`
2. Backend parses `search`, `state`, sort, page, and limit
3. Backend composes filtered query on `sales_data`
4. Backend applies pagination and returns filtered page only

### What user sees after applying filter

- Table shows loading/skeleton/page loader
- Previous in-flight request is cancelled (prevents stale paint)
- New filtered rows replace current page
- Pagination metadata updates (`total`, `totalPages`, current page clamp)

### Filter speed behavior

Fast in common cases because:

- filters run in DB, not browser
- result is paginated (max 200 rows/page in current backend)
- count query uses short TTL cache
- identical page/filter requests can hit page cache
- only required columns are selected

### Slow-case patterns

Filtering may get slower if:

- very broad search text (high cardinality scan)
- expensive count on huge unselective filter
- no supporting index for a filtered/sorted column

### Current performance protection

- bounded page size
- server-side query + pagination only
- request cancellation on UI
- response cache (`X-Data-Cache: HIT/MISS`)

---

---

If needed, this can be extended with:

- API request/response examples
- exact field dictionary for every column
- troubleshooting playbook (slow pages, count lag, filter behavior)
