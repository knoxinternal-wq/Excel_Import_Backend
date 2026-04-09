/**
 * Parallel COPY from CSV shard files (streaming parse + enrich + pg-copy-streams).
 */
import fs from 'fs';
import { parse } from '@fast-csv/parse';
import { getPgPool } from '../../config/database.js';
import {
  withSalesCopyImport,
  SALES_COPY_COLUMNS,
  dropSalesDataGinIndexes,
  rebuildSalesDataGinIndexesPool,
  truncateSalesDataStaging,
  getPartitionName,
} from '../salesCopyInserter.js';
import { enrichImportFactRow } from './importEnrichFactRow.js';

const RN = '__import_rn';

export const CSV_IMPORT_HEADERS = [RN, ...SALES_COPY_COLUMNS];

function slimRowForErrorLog(data) {
  return {
    bill_no: data?.bill_no ?? null,
    bill_date: data?.bill_date != null ? String(data.bill_date) : null,
    item_no: data?.item_no ?? null,
    to_party_name: data?.to_party_name ?? null,
  };
}

function readFirstBillDateFromShardCsv(shardPath) {
  return new Promise((resolve, reject) => {
    let done = false;
    const stream = fs.createReadStream(shardPath);
    const parser = parse({ headers: true, ignoreEmpty: true, trim: true });
    parser.on('data', (row) => {
      if (done) return;
      done = true;
      stream.destroy();
      const v = row?.bill_date;
      resolve(v != null && String(v).trim() !== '' ? String(v).trim() : null);
    });
    parser.on('end', () => {
      if (done) return;
      done = true;
      resolve(null);
    });
    parser.on('error', reject);
    stream.pipe(parser);
  });
}

async function resolveCopyTargetTable(pool, shardPath, multiShard) {
  if (multiShard) return 'sales_data';
  const bill = await readFirstBillDateFromShardCsv(shardPath);
  const part = getPartitionName(bill);
  if (!part || !pool) return 'sales_data';
  try {
    const { rows } = await pool.query('SELECT to_regclass($1)::text AS r', [`public.${part}`]);
    if (rows[0]?.r) return part;
  } catch {
    /* ignore */
  }
  return 'sales_data';
}

/**
 * @param {object} opts
 * @param {string[]} opts.shardPaths
 * @param {number} opts.shardIndex
 * @param {boolean} opts.hasHeaderRow
 * @param {object} opts.job - mutable job state (shared processed sum via perShardRows)
 * @param {object} opts.masters
 * @param {number[]} opts.perShardRows - length N, shard k only writes index k
 * @param {(jobId: string) => Promise<boolean>} opts.isJobCancelled
 * @param {(jobId: string, job: object) => Promise<void>} opts.updateJobInDb
 * @param {number} opts.jobUpdateEveryRows
 * @param {(jobId: string, entries: object[]) => Promise<void>} opts.saveErrorRows
 * @param {number} opts.errorBatchSize
 * @param {number} opts.startedAtMs
 * @param {number} [opts.processedRowsBaseline]
 * @param {number} [opts.failedRowsBaseline]
 * @param {string} [opts.targetTable]
 * @param {boolean} [opts.skipGinToggle]
 */
async function importOneShard({
  shardPath,
  shardIndex,
  hasHeaderRow,
  job,
  masters,
  perShardRows,
  perShardFailed,
  isJobCancelled,
  updateJobInDb,
  jobUpdateEveryRows,
  saveErrorRows,
  errorBatchSize,
  startedAtMs,
  processedRowsBaseline = 0,
  failedRowsBaseline = 0,
  targetTable = 'sales_data',
  skipGinToggle = false,
}) {
  let rowsSinceCheck = 0;
  let rowsSincePersist = 0;
  const errorBuffer = [];

  await withSalesCopyImport(async (writer) => {
    const stream = fs.createReadStream(shardPath);
    const parserOptions = hasHeaderRow
      ? { headers: true, ignoreEmpty: true, trim: true }
      : { headers: CSV_IMPORT_HEADERS, ignoreEmpty: true, trim: true };
    const parser = parse(parserOptions);
    const pipeline = stream.pipe(parser);

    for await (const raw of pipeline) {
      rowsSinceCheck += 1;
      if (rowsSinceCheck >= 2500) {
        rowsSinceCheck = 0;
        if (await isJobCancelled(job.jobId)) {
          job.cancelled = true;
          break;
        }
      }

      const row = { ...raw };
      const rowNumber = Number(row[RN]);
      delete row[RN];
      if (!Number.isFinite(rowNumber)) continue;

      try {
        enrichImportFactRow(row, masters);
        const writeResult = writer.appendRow({ ...row, __rowNumber: rowNumber });
        if (!writeResult.written) {
          perShardFailed[shardIndex] += 1;
          errorBuffer.push({
            rowNumber,
            rowData: slimRowForErrorLog(row),
            errorMessage: writeResult.skipMessage || 'Row skipped for COPY',
          });
        }
        if (writeResult.flushPromise) await writeResult.flushPromise;
      } catch (e) {
        perShardFailed[shardIndex] += 1;
        errorBuffer.push({
          rowNumber,
          rowData: slimRowForErrorLog(row),
          errorMessage: e?.message || String(e),
        });
      }

      perShardRows[shardIndex] += 1;
      rowsSincePersist += 1;
      if (rowsSincePersist >= jobUpdateEveryRows) {
        rowsSincePersist = 0;
        job.processedRows = processedRowsBaseline + perShardRows.reduce((a, b) => a + b, 0);
        const elapsedSec = Math.max((Date.now() - startedAtMs) / 1000, 1);
        job.throughputRps = Number((job.processedRows / elapsedSec).toFixed(2));
        await updateJobInDb(job.jobId, job);
      }

      if (errorBuffer.length >= errorBatchSize) {
        const toInsert = errorBuffer.splice(0, errorBuffer.length);
        await saveErrorRows(job.jobId, toInsert);
      }
    }
  }, { targetTable, skipGinToggle });

  if (errorBuffer.length) {
    await saveErrorRows(job.jobId, errorBuffer);
  }
}

/**
 * @param {object} opts
 * @param {string[]} opts.shardPaths - part-0 has header
 * @param {object} opts.job
 * @param {object} opts.masters
 * @param {number} opts.jobUpdateEveryRows
 * @param {number} [opts.processedRowsBaseline] — rows already persisted (e.g. resumed job)
 * @param {number} [opts.failedRowsBaseline] — failures before COPY phase
 */
export async function importSalesDataFromCsvShards(opts) {
  const {
    shardPaths,
    job,
    masters,
    isJobCancelled,
    updateJobInDb,
    jobUpdateEveryRows,
    saveErrorRows,
    errorBatchSize,
    startedAtMs,
    processedRowsBaseline = 0,
    failedRowsBaseline = 0,
  } = opts;

  const N = shardPaths.length;
  const perShardRows = new Array(N).fill(0);
  const perShardFailed = new Array(N).fill(0);
  const pool = getPgPool();
  const multiShard = N >= 2;

  if (multiShard && pool) {
    const c = await pool.connect();
    try {
      await dropSalesDataGinIndexes(c);
    } finally {
      c.release();
    }
  }

  const singleTarget =
    shardPaths[0] != null
      ? await resolveCopyTargetTable(pool, shardPaths[0], multiShard)
      : 'sales_data';

  const tasks = shardPaths.map((shardPath, shardIndex) =>
    importOneShard({
      shardPath,
      shardIndex,
      hasHeaderRow: shardIndex === 0,
      job,
      masters,
      perShardRows,
      perShardFailed,
      isJobCancelled,
      updateJobInDb,
      jobUpdateEveryRows,
      saveErrorRows,
      errorBatchSize,
      startedAtMs,
      processedRowsBaseline,
      failedRowsBaseline,
      targetTable: multiShard ? 'sales_data' : singleTarget,
      skipGinToggle: multiShard,
    }),
  );

  await Promise.all(tasks);

  if (multiShard && pool) {
    await rebuildSalesDataGinIndexesPool(pool);
    await truncateSalesDataStaging(pool);
  }

  const copyFailed = perShardFailed.reduce((a, b) => a + b, 0);
  const copyProcessed = perShardRows.reduce((a, b) => a + b, 0);
  job.failedRows = failedRowsBaseline + copyFailed;
  job.processedRows = processedRowsBaseline + copyProcessed;
  const elapsedSec = Math.max((Date.now() - startedAtMs) / 1000, 1);
  job.throughputRps = Number((job.processedRows / elapsedSec).toFixed(2));
  await updateJobInDb(job.jobId, job);
}
