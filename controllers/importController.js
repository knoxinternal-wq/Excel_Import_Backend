import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import ExcelJS from 'exceljs';
import { processExcelFile, resumeQueuedImport } from '../services/excelProcessor.js';
import { supabase, supabaseAdmin } from '../models/supabase.js';
import { getPgPool } from '../config/database.js';
import { REQUIRED_HEADERS } from '../config/constants.js';
import { logError, logInfo } from '../utils/logger.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const uploadsDir = path.join(__dirname, '../uploads');
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, uploadsDir);
  },
  filename: (req, file, cb) => {
    const unique = Date.now() + '-' + Math.round(Math.random() * 1e9);
    cb(null, unique + path.extname(file.originalname));
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 200 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    if (ext !== '.xlsx' && ext !== '.xls') {
      return cb(new Error('Only Excel files (.xlsx, .xls) are allowed'));
    }
    cb(null, true);
  },
});

export const uploadMiddleware = upload.single('file');

export async function downloadTemplate(req, res) {
  try {
    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Sales Data', { headerFooter: { firstHeader: 'Sales Data Template' } });
    sheet.addRow(REQUIRED_HEADERS);
    const sampleRow = [
      'Mumbai', '2024', '1', 'Jan', 'West', 'Maharashtra', 'Mumbai', 'Mumbai',
      'Retail', '-', 'Party A', 'Party A', 'Brand X', 'Agent 1', 'Customer A',
      'BILL-001', new Date('2024-01-15'), 'Product A', 'Red', 100, 'M', '10',
      5, 5000, 4500, 4250, 'SO-001', new Date('2024-01-10'), 'Product A - Red',
      'Category 1', 'Sub 1', 'Regular', null, 'Fresh', 'Agent 1', '400001',
    ];
    sheet.addRow(sampleRow);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="sales_data_template.xlsx"');
    await workbook.xlsx.write(res);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

export async function uploadFile(req, res) {
  try {
    if (!req.file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }
    logInfo('import', 'file received', { name: req.file.originalname, bytes: req.file.size });
    const jobId = await processExcelFile(
      req.file.path,
      req.file.originalname,
      req.file.size
    );
    logInfo('import', 'job created', { jobId });
    res.json({ jobId, message: 'Import queued' });
  } catch (err) {
    logError('import', 'upload error', { message: err?.message });
    res.status(500).json({ error: err.message || 'Import failed' });
  }
}

export async function getStatus(req, res) {
  try {
    const { jobId } = req.params;

    /** Prefer direct Postgres so columns added outside PostgREST cache (e.g. checkpoint_row) are visible. */
    const pool = getPgPool();
    let row = null;
    if (pool) {
      const { rows } = await pool.query('SELECT * FROM import_jobs WHERE id = $1 LIMIT 1', [jobId]);
      row = rows[0] || null;
    }
    if (!row) {
      const { data, error } = await supabase
        .from('import_jobs')
        .select('*')
        .eq('id', jobId)
        .single();
      if (error || !data) {
        return res.status(404).json({ error: 'Job not found' });
      }
      row = data;
    }

    if (!row) {
      return res.status(404).json({ error: 'Job not found' });
    }

    const job = {
      jobId: row.id,
      totalRows: row.total_rows || 0,
      processedRows: row.processed_rows || 0,
      failedRows: row.failed_rows || 0,
      checkpointRow: row.checkpoint_row || 0,
      throughputRps: row.throughput_rps || 0,
      status: row.status,
      error: row.error_message,
      filename: row.filename,
      queuedAt: row.queued_at,
      startedAt: row.started_at,
      completedAt: row.completed_at,
    };

    res.set('Cache-Control', 'private, no-store');
    res.json({
      totalRows: job.totalRows,
      processedRows: job.processedRows,
      failedRows: job.failedRows,
      checkpointRow: job.checkpointRow,
      throughputRps: job.throughputRps,
      status: job.status,
      error: job.error,
      filename: job.filename,
      queuedAt: job.queuedAt,
      startedAt: job.startedAt,
      completedAt: job.completedAt,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

export async function cancelImport(req, res) {
  try {
    const { jobId } = req.params;
    const pool = getPgPool();
    let current = null;
    if (pool) {
      const { rows } = await pool.query(
        'SELECT id, status FROM import_jobs WHERE id = $1 LIMIT 1',
        [jobId],
      );
      current = rows[0] || null;
    }
    if (!current) {
      const { data, error: readErr } = await supabase
        .from('import_jobs')
        .select('id, status')
        .eq('id', jobId)
        .single();
      if (readErr || !data) return res.status(404).json({ error: 'Job not found' });
      current = data;
    }
    if (!current) return res.status(404).json({ error: 'Job not found' });
    if (current.status === 'completed' || current.status === 'failed') {
      return res.status(400).json({ error: `Job already ${current.status}` });
    }
    if (pool) {
      await pool.query(
        `UPDATE import_jobs SET cancelled = true, status = 'cancelled' WHERE id = $1`,
        [jobId],
      );
    } else {
      const client = supabaseAdmin || supabase;
      const { error } = await client
        .from('import_jobs')
        .update({ cancelled: true, status: 'cancelled' })
        .eq('id', jobId);
      if (error) throw new Error(error.message);
    }
    res.json({ message: 'Cancel requested', jobId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

export async function resumeImport(req, res) {
  try {
    const { jobId } = req.params;
    const pool = getPgPool();
    let row = null;
    if (pool) {
      const { rows } = await pool.query('SELECT * FROM import_jobs WHERE id = $1 LIMIT 1', [jobId]);
      row = rows[0] || null;
    }
    if (!row) {
      const { data, error } = await supabase
        .from('import_jobs')
        .select('*')
        .eq('id', jobId)
        .single();
      if (error || !data) return res.status(404).json({ error: 'Job not found' });
      row = data;
    }
    if (!row) return res.status(404).json({ error: 'Job not found' });
    if (row.status === 'completed') {
      return res.status(400).json({ error: 'Job already completed' });
    }
    if (pool) {
      await pool.query(
        `UPDATE import_jobs SET cancelled = false, status = 'queued', error_message = NULL WHERE id = $1`,
        [jobId],
      );
    } else {
      const client = supabaseAdmin || supabase;
      const { error: updateErr } = await client
        .from('import_jobs')
        .update({ cancelled: false, status: 'queued', error_message: null })
        .eq('id', jobId);
      if (updateErr) throw new Error(updateErr.message);
    }
    await resumeQueuedImport(jobId);
    res.json({ message: 'Resume requested', jobId });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
