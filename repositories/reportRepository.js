import { getPgPool } from '../config/database.js';

export async function listDistinctFyMonthRows() {
  const pool = getPgPool();
  if (!pool) throw new Error('DATABASE_URL is not configured');
  const { rows } = await pool.query(
    `SELECT DISTINCT fy, month
     FROM sales_data
     WHERE fy IS NOT NULL
       AND month IS NOT NULL
     ORDER BY
       fy ASC,
       CASE split_part(month, '-', 1)
         WHEN 'Jan' THEN 1
         WHEN 'Feb' THEN 2
         WHEN 'Mar' THEN 3
         WHEN 'Apr' THEN 4
         WHEN 'May' THEN 5
         WHEN 'Jun' THEN 6
         WHEN 'Jul' THEN 7
         WHEN 'Aug' THEN 8
         WHEN 'Sep' THEN 9
         WHEN 'Oct' THEN 10
         WHEN 'Nov' THEN 11
         WHEN 'Dec' THEN 12
         ELSE 99
       END ASC,
       month ASC`,
  );
  return rows || [];
}
