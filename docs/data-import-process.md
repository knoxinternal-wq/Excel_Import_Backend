# Data Import Process (Excel → Database)

This document describes how an Excel file moves through the app into **`sales_data`**, **when work actually starts**, **how many rows move at each step**, and **why imports can feel slow**.

---

## 1) User uploads Excel (frontend)

- **UI:** `frontend/src/components/FileUpload.jsx`
- **API:** `POST /api/import` (`multipart/form-data`)
- **Client:** `frontend/src/services/api.js` → `importApi.upload`
- Only **`.xlsx`** / **`.xls`** are allowed (UI + backend).

---

## 2) Backend receives the file

- **Routes:** `backend/routes/import.js` → `uploadMiddleware` + `uploadFile`
- **Controller:** `backend/controllers/importController.js`
- **Multer** saves the file under **`backend/uploads/`**.
- **`uploadFile()`** calls **`processExcelFile(...)`** in `backend/services/excelProcessor.js`.
- Response returns **`jobId`** immediately; heavy work runs **asynchronously** (HTTP is not blocked for the whole file).

---

## 3) What happens immediately after upload (before “heavy” import)

Inside **`processExcelFile()`**:

1. **DB schema guard** — `ensureImportRuntimeSchema()` (adds any missing import-job columns, etc.).
2. A new **`import_jobs`** row is created with status **`queued`** (`updateJobInDb`).
3. The job is **enqueued** on an in-memory queue (`p-queue`): `importJobQueue.add(() => runQueuedImportJob(job))`.
4. **`processExcelFile` returns `jobId`** to the client.

So: **upload finishes quickly**; the **worker may start a few milliseconds later** depending on queue concurrency and other jobs.

---

## 4) When does the “actual” import start?

The real pipeline runs in **`runQueuedImportJob(job)`** (same file). Rough order:

| Step | What happens | User-visible signal |
|------|----------------|---------------------|
| A | If not cancelled → set status **`processing`**, `started_at`, persist job | Status API shows `processing` |
| B | **`loadImportMastersSnapshot()`** (parallel with step C) — all master maps loaded once for in-memory joins | DB reads on master tables |
| C | **One ExcelJS stream pass** — `WorkbookReader` validates headers/rows, emits **CSV lines** to **`uploads/import-tmp/<jobId>/part-0.csv` … `part-(N-1).csv`** (round-robin) | CPU + disk; `checkpoint_row` / throughput updates |
| D | **`importSalesDataFromCsvShards`** — **N parallel** `@fast-csv/parse` readers + **`enrichImportFactRow`** (Node) + **N `COPY sales_data … FROM STDIN`** sessions | Log lines like `COPY session: statement_timeout disabled` |
| E | Temp CSV dir removed, upload file deleted, status **`completed`**, caches invalidated | UI toast / Data refresh |

**Data lands in `sales_data` during step D** (no staging table, no SQL transform).

---

## 5) How many rows at a time? (batching layers)

### A) Excel → CSV shards

- **Streaming:** rows are written **line-by-line** to shard files with **write backpressure** (`drain`); there is no large in-memory row batch for the whole file.
- **Sharding:** **`IMPORT_COPY_PARALLEL`** (default **4**, max **8**) controls how many **`part-*.csv`** files are filled in round-robin order.

### B) COPY stream flush (inside `SalesCopyWriter`)

- File: `backend/services/salesCopyInserter.js`
- CSV lines are buffered per COPY session; the stream flushes when either:
  - **`IMPORT_COPY_ROW_BATCH`** rows are buffered (default **10,000**), or
  - **`IMPORT_COPY_BUFFER_BYTES`** of buffered data (default **64 MiB**)

### C) Progress writes (`import_jobs`)

- **Constant:** `JOB_UPDATE_EVERY_N_ROWS` (env: **`IMPORT_JOB_UPDATE_EVERY_ROWS`**).
- **Default:** **50,000** rows between updates during **parallel COPY** (minimum **5,000** if overridden), plus periodic updates while streaming Excel (throughput / checkpoint).

The UI polls **`GET /api/import/status/:jobId`**; `processed_rows` advances during **COPY** (rows written to **`sales_data`**). During the Excel→CSV phase, **`processed_rows` stays at the resume baseline** while **`checkpoint_row`** and **`throughput_rps`** still move.

### D) Cancel checks

- **Not** every row (that used to be extremely slow).
- **Constant:** `IMPORT_CANCEL_CHECK_EVERY_ROWS` (env: **`IMPORT_CANCEL_CHECK_EVERY_ROWS`**).
- **Default:** every **2,500** rows (Excel pass), **2,500** rows per shard during CSV parse + COPY pipeline.

### E) Failed-row buffer

- **`ERROR_BATCH_SIZE`** from `backend/config/constants.js` (default **50**): error rows are batched before insert into **`import_errors`**.

### F) Queue concurrency

- **`IMPORT_WORKER_CONCURRENCY`** (default **5**): max **concurrent import jobs** in the Node process. Each job uses **multiple parallel COPY clients** (one per shard).

---

## 6) Data path: Excel → CSV shards → parallel COPY → `sales_data`

The **live** worker path is:

1. **`streamExcelToCsvShards`** — validate rows, **`tryBuildCopyCsvLine`** for CSV-safe fact columns, write **`__import_rn`** + columns to shard files.
2. **`importSalesDataFromCsvShards`** — parse each shard with **`@fast-csv/parse`**, **`enrichImportFactRow`** (same business rules as the old SQL transform), then **`withSalesCopyImport`** → **`COPY sales_data`**.

There is a **`withSupabaseBatchImport`** helper in `excelProcessor.js` for REST batch inserts; the **queued worker** uses the path above (direct Postgres **`COPY`** into **`sales_data`**).

---

## 7) Row validation and business rules (per row, in Node)

For **each data row** (streaming, not loaded whole file into RAM):

- Map headers → fields (`EXCEL_HEADER_MAP`, aliases in `constants.js`).
- Parse numbers/dates safely (Excel pass), then again at COPY time where needed (`salesCopyInserter` / `importEnrichFactRow`).
- Enrichment in **`importEnrichFactRow.js`**: `goods_type`, `business_type` (+ Italian Channel / Retailer rule), `so_type`, region/district/pin, party fields, agents, `fy` / `month` / `mmm`, `item_with_shade`, RARE WOOL branch rule, etc. — **no SQL joins on ingest**.

Masters are loaded **once per job** via **`loadImportMastersSnapshot`** (`masterLoaders.js`).

---

## 8) Why import can still feel slow

**Inherent cost**

- **Very large `.xlsx`**: ExcelJS must **parse/stream** millions of cells; that is CPU- and I/O-heavy (mitigated by doing **only one** pass and moving joins out of SQL).
- **Parallel COPY**: total time still depends on Postgres disk/CPU and concurrent sessions.

**Infrastructure**

- **`DATABASE_URL`** on Supabase **pooler (`:6543`)** may not support **`COPY FROM STDIN`** reliably; use **direct Postgres (`:5432`)** for bulk import when possible.

**Previously (fixed) slowness — for context**

- Calling **cancel check on every row** caused **hundreds of thousands** of HTTP requests per file.
- Running **schema `ALTER TABLE` on every progress save** caused huge overhead per batch.

Those patterns were **removed/throttled**; see `excelProcessor.js` and comments around **`IMPORT_CANCEL_CHECK_EVERY_ROWS`** and **`updateJobInDb`**.

---

## 9) Job status, cancel, completion (API + UI)

- **Status:** `GET /api/import/status/:jobId` (also prefers direct Postgres read in controller when pool is available).
- **Polling:** `frontend/src/components/ImportProgress.jsx` (lazy-loaded from `Dashboard`).
- **Cancel:** `POST /api/import/cancel/:jobId` → `cancelled` flag on **`import_jobs`**.
- **After success:** `Dashboard` refreshes data (`dataApi.fetch`), may switch to **Data** tab, **ImportToast** for success/error.

---

## 10) Tables touched

| Table | Role |
|--------|------|
| **`sales_data`** | Final fact rows (direct **`COPY`** target) |
| **`import_jobs`** | Status, progress, checkpoint, throughput |
| **`import_errors`** | Bad/skipped rows |
| Master tables | Read once per job into memory for enrichment (not joined row-by-row in SQL during import) |

---

## 11) Environment knobs (summary)

| Variable | Purpose |
|----------|---------|
| `IMPORT_COPY_PARALLEL` | Shard count / parallel COPY streams (default **4**, max **8**) |
| `IMPORT_JOB_UPDATE_EVERY_ROWS` | Min rows between `import_jobs` progress writes during COPY (default **50000**) |
| `IMPORT_CANCEL_CHECK_EVERY_ROWS` | Rows between cancel checks (default **2500**) |
| `IMPORT_COPY_ROW_BATCH` | Rows per internal COPY buffer flush (default **10000**) |
| `IMPORT_COPY_BUFFER_BYTES` | Max bytes before COPY flush (default **64MB**) |
| `IMPORT_WORKER_CONCURRENCY` | Concurrent import jobs (default **5**) |
| `DATABASE_URL` | Use **direct `5432`** for heaviest throughput when possible |

See **`backend/.env.example`** for the full list.

---

## 12) High-level sequence

1. User uploads file → **`POST /api/import`**
2. File saved → **`import_jobs`** row **`queued`** → job enqueued
3. Worker runs → **`processing`** → masters snapshot + **Excel → CSV shards** under **`uploads/import-tmp/<jobId>/`**
4. **Parallel** fast-csv + enrich + **`COPY`** into **`sales_data`**
5. Remove temp CSV dir → **`completed`** → delete original upload → frontend refresh / toast

---

*Last updated to match `excelProcessor.js`, `importCsvShardsParallel.js`, `importEnrichFactRow.js`, and `salesCopyInserter.js` (parallel COPY into `sales_data`, no staging SQL transform).*
