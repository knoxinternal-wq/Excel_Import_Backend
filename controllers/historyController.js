import { supabase } from '../models/supabase.js';

export async function getImportHistory(req, res) {
  try {
    const limit = Math.min(50, parseInt(req.query.limit) || 20);
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
    const { data: rows, error } = await supabase
      .from('import_errors')
      .select('row_number, row_data, error_message')
      .eq('job_id', jobId)
      .order('row_number');

    if (error) throw new Error(error.message);
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
