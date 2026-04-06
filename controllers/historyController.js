import { supabase } from '../models/supabase.js';
import { getPgPool } from '../config/database.js';

export async function getImportHistory(req, res) {
  try {
    const limit = Math.min(50, parseInt(String(req.query.limit || '20'), 10) || 20);
    const pool = getPgPool();
    if (pool) {
      const { rows } = await pool.query(
        `SELECT id, filename, file_size, total_rows, processed_rows, failed_rows, status, error_message,
                started_at, completed_at, created_at
         FROM import_jobs
         ORDER BY created_at DESC NULLS LAST
         LIMIT $1`,
        [limit],
      );
      return res.json(rows || []);
    }

    const { data, error } = await supabase
      .from('import_jobs')
      .select('id, filename, file_size, total_rows, processed_rows, failed_rows, status, error_message, started_at, completed_at, created_at')
      .order('created_at', { ascending: false })
      .limit(limit);

    if (error) throw new Error(error.message);
    res.json(data || []);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}

export async function downloadFailedRows(req, res) {
  try {
    const { jobId } = req.params;
    const format = req.query.format || 'csv';

    let rows = null;
    const pool = getPgPool();
    if (pool) {
      const r = await pool.query(
        `SELECT row_number, row_data, error_message
         FROM import_errors
         WHERE job_id::text = $1
         ORDER BY row_number`,
        [jobId],
      );
      rows = r.rows || [];
    } else {
      const resQ = await supabase
        .from('import_errors')
        .select('row_number, row_data, error_message')
        .eq('job_id', jobId)
        .order('row_number');
      if (resQ.error) throw new Error(resQ.error.message);
      rows = resQ.data || [];
    }

    if (!rows || rows.length === 0) {
      return res.status(404).json({ error: 'No failed rows found for this import' });
    }

    if (format === 'json') {
      return res.json(rows);
    }

    const csvLines = ['Row Number,Error Message,Row Data'];
    for (const row of rows) {
      const data = row.row_data;
      const dataStr = Array.isArray(data) ? data.join('|') : JSON.stringify(data || {});
      csvLines.push([row.row_number, `"${(row.error_message || '').replace(/"/g, '""')}"`, `"${String(dataStr).replace(/"/g, '""')}"`].join(','));
    }

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename="failed_rows_${jobId}.csv"`);
    res.send(csvLines.join('\n'));
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
}
