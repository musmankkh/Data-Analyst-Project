-- S3 Tables (Iceberg) using CTAS for silver_genome_scores
-- Step 1: Create external table (use regular_s3.sql)
-- Step 2: Create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.silver_genome_scores_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/silver_genome_scores/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.silver_genome_scores;