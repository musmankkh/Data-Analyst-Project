-- Schema DDL Statements for dim_tags
-- Generated: 20251221_140710
-- Source: s3://movielens-elt-project/gold/dim_tags/
-- ============================================================================


-- REGULAR S3
-- ----------------------------------------------------------------------------
-- Regular S3 External Table for dim_tags
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.dim_tags (
  tag_key BIGINT,
  tag_name STRING,
  tag_source STRING,
  tag_length BIGINT,
  word_count BIGINT,
  tag_category STRING,
  tag_sentiment STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/dim_tags/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);


-- S3 TABLES CTAS
-- ----------------------------------------------------------------------------
-- S3 Tables (Iceberg) using CTAS for dim_tags
-- Step 1: Create external table (use regular_s3.sql)
-- Step 2: Create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.dim_tags_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/dim_tags/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.dim_tags;


-- S3 TABLES DIRECT
-- ----------------------------------------------------------------------------
-- Direct S3 Tables (Iceberg) creation for dim_tags
CREATE TABLE s3_tables_catalog.database_name.dim_tags_iceberg (
  tag_key BIGINT,
  tag_name STRING,
  tag_source STRING,
  tag_length BIGINT,
  word_count BIGINT,
  tag_category STRING,
  tag_sentiment STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
LOCATION 's3://your-s3-tables-bucket/dim_tags/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.dim_tags_iceberg
SELECT * FROM database_name.dim_tags;

