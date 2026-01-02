-- S3 Tables (Iceberg) using CTAS for kpi_genre_performance
-- Step 1: Create external table (use regular_s3.sql)
-- Step 2: Create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.kpi_genre_performance_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/kpi_genre_performance/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.kpi_genre_performance;