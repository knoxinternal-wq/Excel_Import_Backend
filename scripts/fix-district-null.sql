-- Clear wrong district data in sales_data
-- District was being populated from wrong Excel columns (LEVEL1, CITY CODE, LEVEL).
-- Run in Supabase SQL Editor to set district = null for all rows.

UPDATE sales_data SET district = NULL WHERE district IS NOT NULL;
