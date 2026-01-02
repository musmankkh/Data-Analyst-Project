-- Schema DDL Statements for silver_genome_tags
-- Generated: 20251221_132625
-- Source: s3://movielens-elt-project/silver/silver_genome_tags/
-- ============================================================================


-- REGULAR S3
-- ----------------------------------------------------------------------------
-- Regular S3 External Table for silver_genome_tags
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.silver_genome_tags (
  tag_id BIGINT,
  tag STRING,
  tag_original STRING,
  tag_length BIGINT,
  is_tag_missing BOOLEAN,
  is_tag_too_short BOOLEAN,
  is_tag_too_long BOOLEAN,
  has_special_chars BOOLEAN,
  is_duplicate BOOLEAN,
  duplicate_rank BIGINT,
  loaded_at TIMESTAMP,
  dbt_run_id STRING,
  dbt_run_started_at STRING
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/silver/silver_genome_tags/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);


-- S3 TABLES CTAS
-- ----------------------------------------------------------------------------
-- S3 Tables (Iceberg) using CTAS for silver_genome_tags
-- Step 1: Create external table (use regular_s3.sql)
-- Step 2: Create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.silver_genome_tags_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/silver_genome_tags/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.silver_genome_tags;


-- S3 TABLES DIRECT
-- ----------------------------------------------------------------------------
-- Direct S3 Tables (Iceberg) creation for silver_genome_tags
CREATE TABLE s3_tables_catalog.database_name.silver_genome_tags_iceberg (
  tag_id BIGINT,
  tag STRING,
  tag_original STRING,
  tag_length BIGINT,
  is_tag_missing BOOLEAN,
  is_tag_too_short BOOLEAN,
  is_tag_too_long BOOLEAN,
  has_special_chars BOOLEAN,
  is_duplicate BOOLEAN,
  duplicate_rank BIGINT,
  loaded_at TIMESTAMP,
  dbt_run_id STRING,
  dbt_run_started_at STRING
)
LOCATION 's3://your-s3-tables-bucket/silver_genome_tags/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.silver_genome_tags_iceberg
SELECT * FROM database_name.silver_genome_tags;

