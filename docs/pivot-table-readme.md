# Pivot Table Architecture and Data Flow

This document explains how the Pivot Table (**Report** tab) works in this project:

- UI behavior
- request/response flow (including **detailed API and env tables**)
- backend aggregation logic (Postgres vs stream, filters, `pivotSql.js`)
- drilldown/export flow
- performance, body paging, and caching

It is intended as an engineering reference for scaling and maintenance. The top-level project overview also links here from `README.md` (section *Report tab (Pivot): architecture and data flow*).

---

## 1) Purpose of the Pivot Table

The Pivot Table provides Excel-style analysis over `sales_data` without loading full raw data into the browser.

Core goals:

- backend-first aggregation (not client-side heavy compute)
- support large datasets (500k+ rows)
- responsive interaction for rows/columns/filters/values changes
- export-ready output (CSV/XLSX)

---

## 2) Frontend Entry and Main Components

### Main UI entry

- `frontend/src/pages/Dashboard.jsx`
  - `report` tab renders `PivotReport`

### Pivot UI implementation

- `frontend/src/components/pivot/PivotReport.jsx`

This component handles:

- field loading
- rows/columns/values/filter configuration
- drag/drop pivot field layout
- debounced filter apply
- calling backend pivot APIs
- rendering large pivot body using virtualization (`react-window`)
- drilldown/export/copy actions

---

## 3) Pivot UI Data Flow (Frontend)

### Step A: Load available fields

On mount:

1. `pivotApi.fields()` -> `GET /api/data/report/fields`
2. UI builds field list (dimension/measure metadata)
3. restores saved config from localStorage if available

### Step B: User configures pivot

User builds config using:

- **Rows** (dimensions)
- **Columns** (dimensions)
- **Values** (measures + aggregation `sum/count/avg/min/max`)
- **Filters** (field + operator + value(s))

### Step C: Debounced run

- Filters are debounced (`useDebouncedValue`) to avoid API spam
- `runPivot()` sends config to backend:
  - `pivotApi.run(payload)` -> `POST /api/data/report/pivot`
- Payload typically includes **`bodyOffset` / `bodyLimit`** (default chunk **100** rows) for the first paint; scrolling requests the next chunk (see §4.2)

### Step D: Render result

Backend response includes:

- row headers
- column headers
- body cells (and/or windowed **body line** representation when body paging is used)
- row totals / column totals / grand totals
- subtotals and **metadata** (`meta.engine`, row counts, etc.)

Frontend then:

- creates pivot layout headers
- renders body with virtualized rows (`react-window`)
- merges additional body pages when the user scrolls for “load more”
- renders sticky-style summary/footer totals section

---

## 4) API Endpoints Used by Pivot

All routes are mounted from `backend/routes/data.js` under the **`/api/data`** prefix (full paths below).

### 4.1 Endpoint reference (detailed)

| Method | Path | Purpose | Typical request | Response highlights |
|--------|------|---------|-----------------|----------------------|
| `GET` | `/api/data/report/meta` | Report metadata (shared with other report UI) | Query params if any | Metadata JSON |
| `GET` | `/api/data/report/fields` | Valid pivot field list + types | None | `{ fields: [...] }` — dimension vs measure, default agg |
| `GET` | `/api/data/report/filter-values` | DISTINCT values for **one** filter field | `?field=&search=&limit=` | `{ field, values: string[] }` — ordered, trimmed text |
| `POST` | `/api/data/report/filter-values-batch` | DISTINCT values for **many** fields in one round-trip | JSON `{ fields: string[], limit?: number }` | `{ fields: { [field]: { values } \| { error } } }` |
| `POST` | `/api/data/report/pivot` | Run pivot aggregation | JSON pivot config + optional body window (see §4.2) | Matrix: headers, cells, totals, `meta.engine`, optional `bodyLines` |
| `POST` | `/api/data/report/drilldown` | Raw fact rows for a cell | JSON `{ config, drill: { rowKey, columnKey, offset, limit } }` | `{ total, rows, offset, limit }` |
| `POST` | `/api/data/report/export` | Full pivot → file | JSON `{ config, format: 'csv' \| 'xlsx' }` | Binary CSV or XLSX (body window stripped server-side) |
| `GET` | `/api/data/pivot` | Quick preset pivot (charts / links) | Query: `rows`, `columns`, `field`, `agg`, etc. | Lightweight pivot JSON |

**Handlers** (for code navigation): `getPivotFieldsHandler`, `getPivotFilterValuesHandler`, `getPivotFilterValuesBatchHandler`, `getPivotDataHandler`, `getPivotDrilldownHandler`, `exportPivotHandler`, `getPivotQuickHandler` in `backend/controllers/pivotController.js`.

### 4.2 `POST /api/data/report/pivot` — body fields (detailed)

These fields sit **alongside** the pivot layout (`rows`, `columns`, `values`, `filters`, `sort`, …).

| Field | Type | Purpose |
|-------|------|---------|
| `bodyOffset` | number | Zero-based index into the **ordered row-axis body lines**; used with `bodyLimit` for progressive loading. |
| `bodyLimit` | number | Max body lines to return in this response (capped server-side, e.g. 100k). Default UI page size is **100** rows per chunk (`PivotReport.jsx`). |
| `subtotalFields` | string[] | Row dimensions at which to compute **subtotal** rows in the body ordering pipeline (`pivotBodyOrder.js`). |

**Flow:** `runPivot(config)` computes the **full** pivot. `getPivotDataHandler` then calls **`applyPivotBodyWindow`** so the JSON may include a **windowed** `bodyLines` (or equivalent structure) while **row/column/grand totals** still reflect the full filtered result.

**Export** always uses a config **without** `bodyOffset` / `bodyLimit` so the file contains the complete body.

### 4.3 Client timeouts (frontend)

| Concern | Where | Notes |
|---------|--------|------|
| General fast APIs | `frontend/src/constants/timing.js` → `MAX_UI_DATA_LOAD_MS` | Used for fields, some admin calls; env: `VITE_API_FAST_TIMEOUT_MS`. |
| Pivot body / long POST | `LONG_RUNNING_REQUEST_MS` | `pivotApi.run`, export, drilldown; env: `VITE_API_LONG_TIMEOUT_MS`. |
| Filter DISTINCT | `PIVOT_FILTER_VALUES_TIMEOUT_MS` | Must be **≥** server `PIVOT_FILTER_SQL_TIMEOUT_MS` or the browser aborts first; env: `VITE_PIVOT_FILTER_TIMEOUT_MS`. |

---

## 5) Backend Controller Flow

Main file: `backend/controllers/pivotController.js`

### `getPivotDataHandler`

| Step | What happens |
|------|----------------|
| 1 | Parse JSON body: pivot config + optional `bodyOffset`, `bodyLimit`, `subtotalFields` (`parsePivotBodyWindow`). |
| 2 | Call `runPivot(body)` — **full** aggregation in `pivotService.js`. |
| 3 | If `bodyLimit` set, apply **`applyPivotBodyWindow`** (`pivotBodyOrder.js`) so the response includes only the requested slice of body lines while totals stay global. |
| 4 | Serialize cells / totals through `toDisplayNumber` for stable JSON numbers in the UI. |
| 5 | Return pivot payload + `meta` (includes `engine`: `postgres` vs `stream`). |

### Error mapping (SQL timeouts)

| Condition | HTTP | Payload |
|-----------|------|---------|
| PostgreSQL statement timeout (`57014` or message match) | **504** | `{ error, code: 'PIVOT_TIMEOUT' }` — message indicates **database-side** timeout; pivot aggregation uses `SET LOCAL statement_timeout = 0` in app code, so remaining limits are usually **host/DB/pooler**. |
| Other pivot errors | **400** | `{ error: err.message }` |

### `getPivotDrilldownHandler`

1. Receive `{ config, drill }`
2. Call `runDrilldown(config, drill)` — SQL path first when eligible, else stream
3. Return paged rows for selected pivot intersection

### `exportPivotHandler`

1. **`stripPivotBodyWindowParams`** removes `bodyOffset` / `bodyLimit` from config
2. `runPivot(fullConfig)` — complete matrix
3. Build export grid (header/body/footer); XLSX via `ExcelJS`
4. Return CSV or XLSX stream

---

## 6) Backend Service Logic

Main file: `backend/services/pivotService.js`

This file contains the heavy logic.

### A) Config normalization

- validates `rows/columns/values/filters`
- keeps only allowed fields from `SALES_FIELDS`
- enforces safe row limits (`MAX_SALES_ROWS`)

### B) Filter strategy split (critical for speed)

`splitFilters()` in `pivotService.js` partitions filters into:

| Bucket | Applied where | Typical contents |
|--------|----------------|------------------|
| **`sqlFilters`** | Postgres `WHERE` in `pivotSql.js` **and** Supabase query in stream path | `eq`, `in`, `contains`, numeric compares, dates, **`is_blank` / `is_not_blank`**, empty-string equality on text — when **`DATABASE_URL`** pool exists, most of these stay in SQL so **`memFilters` stays empty** and SQL pivot stays eligible. |
| **`memFilters`** | In-process only during **`runPivotWithStream`** | Operators that cannot be expressed for the current path (e.g. no pool + huge string `IN` for PostgREST), or non-numeric compares on non-numeric fields. |

**Rule of thumb:** If `memFilters.length > 0`, **`isPivotSqlAggregationEligible`** returns false → **stream** engine.

### C) Two aggregation engines

#### 1. SQL-first aggregation path (fast path)

Used when eligible (`isPivotSqlAggregationEligible` in `pivotSql.js`):

| Requirement | Detail |
|-------------|--------|
| Pool | `getPgPool()` / `DATABASE_URL` must be set |
| `memFilters` | Must be **empty** |
| Dimensions | `rows.length + columns.length` ≤ **`PIVOT_MAX_GROUP_DIMENSIONS`** (env, **default 32** if unset) |
| Values | Supported aggs (`sum`, `count`, `avg`, `min`, `max`) on allowed fields; `id` + `count` only special case |

Execution:

- `runPivotWithPostgres(...)` → `queryPivotGroupBy(...)` (or materialized-view shortcuts when layout matches)
- `pivotSql.js` runs inside **`withPivotSqlClient`**: `BEGIN`, optional `work_mem` / parallel workers, then grouped SQL on `sales_data`.

Benefits:

- low app CPU
- fewer transferred rows
- best performance on large tables

#### 2. Stream fallback path

Used when SQL eligibility fails:

- `runPivotWithStream(...)`
- streams pages from `sales_data` by cursor (`id > lastId`)
- applies **`memFilters`** in Node and aggregates incrementally

Benefits:

- correctness fallback when SQL path is not eligible
- memory-safe page iteration (still capped by `MAX_SALES_ROWS` scan safety)

### D) Totals and subtotals

After aggregation:

- constructs row totals
- column totals
- grand totals
- row subtotals by configured depth

### E) Drilldown logic

`runDrilldown(...)`:

- tries SQL drilldown fast path first
- falls back to stream filtering if required
- always paginates drilldown rows

### F) SQL execution layer (`pivotSql.js`)

| Mechanism | Behavior |
|-----------|----------|
| **`withPivotSqlClient`** | Acquires pool client, `BEGIN`, sets **`SET LOCAL statement_timeout`** per transaction. |
| **Pivot aggregation** (`queryPivotGroupBy`, MV paths) | Timeout is **`0` (disabled)** — the app does **not** impose a finite cap on heavy `GROUP BY`; only DB/pooler limits apply. |
| **Supporting SQL** (DISTINCT filter values, drilldown queries) | Uses **`PIVOT_FILTER_SQL_TIMEOUT_MS`** (default **180000** ms); optional **`0`** = off. |
| **Session tuning** | Optional `SET LOCAL work_mem` / `max_parallel_workers_per_gather` from **`PIVOT_PG_WORK_MEM`**, **`PIVOT_PG_PARALLEL_WORKERS`**. |
| **Grouped query shape** | `SELECT` dimension expressions (`BTRIM` text dims, date casts, derived FY/month from `bill_date` where applicable) + measure aggregates over **`sales_data sd`** + `WHERE` from `buildWhereFromSqlFilters`. |
| **MV fast paths** | When layout + measures match (e.g. brand×state×month MV, optional `sales_pivot_mv`), read pre-aggregated relation first; **fallback** to live `sales_data` on error. |
| **DISTINCT filters** | `SELECT DISTINCT BTRIM(col::text) … ORDER BY 1 LIMIT n` — aligns with expression indexes centralized in `backend/models/schema.sql`. |
| **Batch filter values** | `getFilterValuesBatch` in `pivotService.js` runs multiple fields with **bounded concurrency** to avoid N simultaneous heavy scans. |

Response metadata: check **`meta.engine`** — **`postgres`** vs **`stream`**.

---

## 7) Caching Strategy

### Frontend cache

In `PivotReport.jsx`:

- small in-memory response cache (`pivotResponseCacheRef`)
- TTL-based reuse for repeated same config
- avoids repeated network calls during rapid toggling

Also caches in browser storage:

- `pivot_report_config_v1` (config)
- `pivot_report_result_v1` (last result snapshot)

### Backend cache

In `pivotService.js`:

- in-process pivot result cache (`pivotResultCache`)
- Redis cache integration when configured:
  - `pivotRedisGet`
  - `pivotRedisSet`

Result: repeated same pivot configs can return fast without recomputation.

---

## 8) Performance Features for Large Data

### 1) Backend-first compute

- pivot aggregation done server-side
- frontend only renders returned matrix

### 2) SQL path preference

- if config is SQL-eligible, uses grouped SQL
- avoids loading raw rows into app process

### 3) Stream-safe fallback

- cursor paging by `id` for fallback
- bounded memory even on very large datasets

### 4) Virtualized body render

- large pivot body uses `react-window List`
- only visible rows are mounted

### 5) Body-window mode

| Aspect | Detail |
|--------|--------|
| Default page size | **100** body lines per request (`PIVOT_BODY_PAGE_SIZE` in `PivotReport.jsx`) |
| Scroll to load more | Increments `bodyOffset`; client merges pages (`mergePivotBodyPages`) |
| Totals | Computed on **full** pivot before slicing; windowing is **presentation-only** (`applyPivotBodyWindow`) |
| “Load all” | UI can request larger or unlimited body window where supported |

### 6) Distinct filter value optimization

| Layer | What it does |
|-------|----------------|
| **Batch API** | `POST /api/data/report/filter-values-batch` loads many active filter fields in one HTTP call |
| **Server concurrency** | `getFilterValuesBatch` uses chunked parallel DISTINCT queries (not unbounded `Promise.all`) |
| **SQL** | `queryDistinctPivotFilterValues` via pooled Postgres; timeout from **`PIVOT_FILTER_SQL_TIMEOUT_MS`** |
| **Server memory cache** | TTL map in `pivotSql.js` (`PIVOT_FILTER_VALUES_CACHE_TTL_MS`, default 30 min) |
| **Redis** | Optional cache for filter lists when `REDIS_URL` / `REDISCLOUD_URL` configured |
| **Browser cache** | `api.js` caches per field/search/limit; **batch** responses also warm per-field keys |

---

## 9) Pivot Request/Response Shape (Simplified)

### Request (example)

```json
{
  "rows": ["region"],
  "columns": ["month"],
  "values": [{ "field": "net_amount", "agg": "sum" }],
  "filters": {
    "state": { "operator": "in", "values": ["RAJASTHAN", "GUJARAT"] }
  },
  "sort": { "rows": "asc", "columns": "asc" },
  "limitRows": 1000000,
  "bodyOffset": 0,
  "bodyLimit": 100,
  "subtotalFields": []
}
```

`bodyOffset` / `bodyLimit` / `subtotalFields` are optional; omit them to receive the full body in one response (heavier for large row axes).

### Response (high-level)

```json
{
  "config": { "...": "..." },
  "values": [...],
  "rowHeaders": [...],
  "columnHeaders": [...],
  "cells": { "...": "..." },
  "rowTotals": { "...": "..." },
  "columnTotals": { "...": "..." },
  "grandTotals": { "...": "..." },
  "rowSubtotals": [...],
  "meta": {
    "sourceRows": 500000,
    "filteredRows": 245812,
    "visibleCells": 4380,
    "engine": "postgres"
  },
  "bodyLines": []
}
```

`engine` is **`postgres`** (SQL `GROUP BY`) or **`stream`** (cursor + in-memory aggregate). `bodyLines` may be present when the controller applies a body window; exact shape matches the current `getPivotDataHandler` serialization.

---

## 10) UI Layout Model (Excel/Power BI style)

`PivotReport.jsx` follows this model:

- **Right panel**: field list + drag/drop zones (Filters/Columns/Rows/Values)
- **Center area**: pivot table grid
- **Top rows**: filter chips, KPIs (`sourceRows`, `filteredRows`, `cells`)
- **Footer controls**: export/copy actions

This keeps configuration and result in one screen for analyst workflow.

---

## 11) Drilldown Flow

1. User clicks pivot cell (rowKey + columnKey)
2. Frontend sends drill payload to `/report/drilldown`
3. Backend returns only matching rows, paged
4. UI can display detailed records without fetching full dataset

This prevents huge raw result transfer during analysis.

---

## 12) Export Flow

### CSV export

- built from backend pivot matrix
- numeric formatting aligned for readable business reports

### XLSX export

- structured headers + totals
- styled cells and frozen panes for usability

Exports are computed from backend result state, not from fragile DOM scraping.

---

## 13) Guardrails and Limits

| Limit | Where | Purpose |
|-------|--------|---------|
| `MAX_SALES_ROWS` | `pivotService.js` stream path | Caps how many fact rows the stream engine will scan (safety). |
| SQL dimension cap | `PIVOT_MAX_GROUP_DIMENSIONS` (4–32, **default 32** if unset) | Max row+column fields for **Postgres** pivot eligibility. |
| Value specs | `MAX_VALUE_SPECS` (12) in `pivotSql.js` | Max number of measure definitions in one pivot. |
| Drilldown page size | Capped in `queryDrilldownPage` (e.g. max 1000) | Prevents huge single responses. |
| Filter DISTINCT limit | Request `limit` (UI uses **500** default for batch/single) | Bounds dropdown size and query cost. |
| Export / body window | `MAX_PIVOT_BODY_LIMIT` in controller | Upper bound for `bodyLimit` if used in other contexts. |
| Pivot matrix size | UI checks on cell counts | Avoids browser OOM on extreme grids |

These guardrails keep the system stable under enterprise-scale data.

---

## 14) Environment Variables and Tuning (table)

See also `backend/.env.example` for full comments.

| Variable | Default / notes | Effect |
|----------|-----------------|--------|
| `DATABASE_URL` | Required for SQL pivot | Enables `getPgPool()`, Postgres DISTINCT, `GROUP BY` path. |
| `NODE_ENV` / `RENDER` | — | Used elsewhere; pivot aggregation **does not** shorten timeout based on these (aggregation `statement_timeout = 0`). |
| `PIVOT_MAX_GROUP_DIMENSIONS` | **32** if unset (allowed range 4–32) | Max row+column dimensions for SQL pivot. |
| `PIVOT_FILTER_SQL_TIMEOUT_MS` | **180000** | Statement timeout for **filter DISTINCT** + **drilldown** SQL only. |
| `PIVOT_FILTER_VALUES_CACHE_TTL_MS` | **30 min** | In-process cache TTL for DISTINCT lists. |
| `PIVOT_FILTER_VALUES_CACHE_MAX` | **100** | Max entries in DISTINCT in-memory cache. |
| `PIVOT_MEMORY_CACHE_TTL_MS` | **180000** | In-process pivot **result** cache TTL (`pivotService.js`). |
| `PIVOT_PG_WORK_MEM` | unset | e.g. `256MB` — `SET LOCAL work_mem` for pivot transaction. |
| `PIVOT_PG_PARALLEL_WORKERS` | unset | 0–8 — `SET LOCAL max_parallel_workers_per_gather`. |
| `PIVOT_PG_SKIP_FACT_ROW_COUNT` | default skip second count on some MV paths | See `pivotSql.js` brand/state/month MV block. |
| `REDIS_URL` / `REDISCLOUD_URL` | optional | Enables Redis for pivot + filter caches (`pivotRedisCache.js`). |
| **Frontend** `VITE_API_LONG_TIMEOUT_MS` | long pivot/export | Must cover slow pivots if DB is heavy. |
| **Frontend** `VITE_PIVOT_FILTER_TIMEOUT_MS` | default **180s** | Should be ≥ `PIVOT_FILTER_SQL_TIMEOUT_MS`. |

**Operational tips (500k+ rows):**

- Keep **`meta.engine === postgres`** — use filters expressible in SQL and ensure `DATABASE_URL` is set.
- Ensure the latest **`backend/models/schema.sql`** is applied on production so DISTINCT/filter indexes and MVs stay in sync.
- If queries still cancel, investigate **Supabase statement timeout**, **pooler**, and **indexes** — not app `PIVOT_PG_STATEMENT_TIMEOUT_MS` (removed for aggregation).

---

## 15) Key Files Reference

- `frontend/src/components/pivot/PivotReport.jsx`
- `frontend/src/services/api.js`
- `backend/routes/data.js`
- `backend/controllers/pivotController.js`
- `backend/services/pivotService.js`
- `backend/services/pivotSql.js`
- `backend/services/pivotRedisCache.js`
- `backend/services/pivotBodyOrder.js`

---

## 16) Summary

Pivot in this system is architected as **backend-compute + virtualized UI render + optional body paging**, which is the right pattern for enterprise scale.

Fast path:

- SQL `GROUP BY` on Postgres (`meta.engine === postgres`), optional MV shortcuts, **`statement_timeout = 0`** for aggregation (no app-side cap on query duration)
- Filter dropdowns via **batch DISTINCT** + caches
- Cache hits (browser / in-process / Redis) + virtualized rendering

Fallback path:

- Streamed in-app aggregation with bounded memory (`meta.engine === stream`)

This design avoids UI lag, avoids full dataset transfer, and supports large imported volumes reliably. Remaining timeouts, if any, are typically **database or pooler** policy—not `PIVOT_PG_STATEMENT_TIMEOUT_MS` on pivot aggregation.

---

## 17) How Pivot Filters Work (Detailed)

### A) Filter UI behavior

In `PivotReport.jsx`, each filter has:

- field
- operator (`contains`, `eq`, `in`, `gt/gte/lt/lte`, `is_blank`, `is_not_blank`)
- single value or multi-values

Frontend behavior:

- filter change is debounced before rerun
- distinct values are fetched **per field** or via **`filterValuesBatch`** for all active filter fields
- active filters appear as chips in report header

### B) What happens after filter apply

1. Debounced filter state is converted to payload
2. `POST /api/data/report/pivot` is called
3. Backend normalizes config and splits filters into SQL vs memory filters
4. Pivot is recomputed and UI refreshes matrix/totals

User-visible effect:

- loading overlay (`Updating pivot...`)
- new filtered result grid
- updated KPIs (`sourceRows`, `filteredRows`, `cells`)

### C) Why filter speed is usually fast

- SQL-eligible filters are pushed to DB (`WHERE` before aggregation)
- grouped SQL path returns compact aggregate data
- repeated same filter set can hit:
  - frontend short-term cache
  - backend in-process cache
  - Redis cache (if enabled)

### D) Why some filters are slower

Fallback to **stream** (`meta.engine === stream`) still happens when:

| Cause | Detail |
|-------|--------|
| **`memFilters` non-empty** | e.g. no Postgres pool, or operators that cannot go to SQL for the active path |
| **SQL not eligible** | Too many row+column dims vs `PIVOT_MAX_GROUP_DIMENSIONS`, or unsupported measure mix |
| **Large unselective filters** | Still correct but more rows to aggregate in stream path |

**Note:** With a pool, **blank / not blank / empty text equality** are generally kept in **SQL** (not `memFilters`), so SQL pivot stays enabled more often than in older builds.

When the stream path runs:

- rows are scanned in cursor pages (`id` keyset)
- memory stays bounded (`MAX_SALES_ROWS`)
- latency is usually higher than grouped SQL on large facts

### E) Filter values dropdown speed

| Path | Detail |
|------|--------|
| **Batch** | `POST /api/data/report/filter-values-batch` — preferred when many filters are open; one HTTP round-trip |
| **Single** | `GET /api/data/report/filter-values?field=&search=&limit=` — lazy/single-field loads |
| **Server** | Postgres `DISTINCT` + `PIVOT_FILTER_SQL_TIMEOUT_MS`; bounded batch concurrency in `getFilterValuesBatch` |
| **Caches** | Server in-memory + optional Redis; client `api.js` TTL cache + inflight deduplication |

### F) Practical speed expectations

| Scenario | Expectation |
|----------|-------------|
| Selective filters + SQL pivot + indexes | Usually fastest (often seconds or less on warm DB) |
| Broad filters + high cardinality GROUP BY | Longer SQL time; no app aggregation timeout — bounded by DB |
| Stream fallback | Slower; scales with scanned rows |
| Repeated same config / filter lists | Frontend + backend + Redis caches help |
