-- Schema DDL Statements
-- Generated: 20251220_155425
-- Source: s3://movielens-elt-project/bronze_layer/
-- ============================================================================


-- REGULAR S3
-- ----------------------------------------------------------------------------
-- Regular S3 External Table
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.bronze_layer (
  movieId BIGINT,
  tagId BIGINT,
  relevance DOUBLE,
  _ingestion_timestamp TIMESTAMP,
  _source_file STRING,
  _source_bucket STRING
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/bronze_layer/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);


-- S3 TABLES CTAS
-- ----------------------------------------------------------------------------
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


-- S3 TABLES DIRECT
-- ----------------------------------------------------------------------------
-- Direct S3 Tables (Iceberg) creation
CREATE TABLE s3_tables_catalog.database_name.bronze_layer_iceberg (
  movieId BIGINT,
  tagId BIGINT,
  relevance DOUBLE,
  _ingestion_timestamp TIMESTAMP,
  _source_file STRING,
  _source_bucket STRING
)
LOCATION 's3://your-s3-tables-bucket/bronze_layer/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.bronze_layer_iceberg
SELECT * FROM database_name.bronze_layer;

