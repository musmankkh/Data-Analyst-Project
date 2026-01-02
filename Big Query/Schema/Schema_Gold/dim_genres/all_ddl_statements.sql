-- Schema DDL Statements for dim_genres
-- Generated: 20251221_140710
-- Source: s3://movielens-elt-project/gold/dim_genres/
-- ============================================================================


-- REGULAR S3
-- ----------------------------------------------------------------------------
-- Regular S3 External Table for dim_genres
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.dim_genres (
  genre_key BIGINT,
  genre_name STRING,
  genre_super_category STRING,
  genre_mood STRING,
  target_audience STRING,
  typical_content_rating STRING,
  typical_setting STRING,
  created_At TIMESTAMP,
  updated_At TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/dim_genres/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);


-- S3 TABLES CTAS
-- ----------------------------------------------------------------------------
-- S3 Tables (Iceberg) using CTAS for dim_genres
-- Step 1: Create external table (use regular_s3.sql)
-- Step 2: Create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.dim_genres_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/dim_genres/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.dim_genres;


-- S3 TABLES DIRECT
-- ----------------------------------------------------------------------------
-- Direct S3 Tables (Iceberg) creation for dim_genres
CREATE TABLE s3_tables_catalog.database_name.dim_genres_iceberg (
  genre_key BIGINT,
  genre_name STRING,
  genre_super_category STRING,
  genre_mood STRING,
  target_audience STRING,
  typical_content_rating STRING,
  typical_setting STRING,
  created_At TIMESTAMP,
  updated_At TIMESTAMP
)
LOCATION 's3://your-s3-tables-bucket/dim_genres/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.dim_genres_iceberg
SELECT * FROM database_name.dim_genres;

