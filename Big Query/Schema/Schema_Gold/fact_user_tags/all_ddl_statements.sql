-- Schema DDL Statements for fact_user_tags
-- Generated: 20251221_140710
-- Source: s3://movielens-elt-project/gold/fact_user_tags/
-- ============================================================================


-- REGULAR S3
-- ----------------------------------------------------------------------------
-- Regular S3 External Table for fact_user_tags
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.fact_user_tags (
  tag_event_key BIGINT,
  user_id BIGINT,
  movie_id BIGINT,
  tag_key BIGINT,
  date_key BIGINT,
  tag STRING,
  tag_type STRING,
  tag_original STRING,
  tag_length BIGINT,
  tag_word_count BIGINT,
  tagged_year BIGINT,
  tagged_month BIGINT,
  tagged_at TIMESTAMP,
  tagged_date DATE,
  tag_complexity STRING,
  source_loaded_at TIMESTAMP,
  created_at TIMESTAMP,
  source_table STRING
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/fact_user_tags/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);


-- S3 TABLES CTAS
-- ----------------------------------------------------------------------------
-- S3 Tables (Iceberg) using CTAS for fact_user_tags
-- Step 1: Create external table (use regular_s3.sql)
-- Step 2: Create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.fact_user_tags_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/fact_user_tags/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.fact_user_tags;


-- S3 TABLES DIRECT
-- ----------------------------------------------------------------------------
-- Direct S3 Tables (Iceberg) creation for fact_user_tags
CREATE TABLE s3_tables_catalog.database_name.fact_user_tags_iceberg (
  tag_event_key BIGINT,
  user_id BIGINT,
  movie_id BIGINT,
  tag_key BIGINT,
  date_key BIGINT,
  tag STRING,
  tag_type STRING,
  tag_original STRING,
  tag_length BIGINT,
  tag_word_count BIGINT,
  tagged_year BIGINT,
  tagged_month BIGINT,
  tagged_at TIMESTAMP,
  tagged_date DATE,
  tag_complexity STRING,
  source_loaded_at TIMESTAMP,
  created_at TIMESTAMP,
  source_table STRING
)
LOCATION 's3://your-s3-tables-bucket/fact_user_tags/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.fact_user_tags_iceberg
SELECT * FROM database_name.fact_user_tags;

