-- S3 Tables (Iceberg format) using CTAS
-- Step 1: First create the external table above
-- Step 2: Then create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.bronze_layer_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/bronze_layer/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.bronze_layer;