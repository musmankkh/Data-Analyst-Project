-- Schema DDL Statements for silver_movies
-- Generated: 20251221_132625
-- Source: s3://movielens-elt-project/silver/silver_movies/
-- ============================================================================


-- REGULAR S3
-- ----------------------------------------------------------------------------
-- Regular S3 External Table for silver_movies
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.silver_movies (
  movie_id BIGINT,
  title STRING,
  title_clean STRING,
  release_year BIGINT,
  content_type STRING,
  genres STRING,
  genres_array STRING,
  genre_count BIGINT,
  is_title_missing BOOLEAN,
  is_genre_missing BOOLEAN,
  has_malformed_year BOOLEAN,
  has_invalid_year BOOLEAN,
  year_pattern_type STRING,
  has_invalid_genre BOOLEAN,
  is_duplicate BOOLEAN,
  duplicate_rank BIGINT,
  loaded_at TIMESTAMP,
  dbt_run_id STRING,
  dbt_run_started_at STRING
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/silver/silver_movies/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);


-- S3 TABLES CTAS
-- ----------------------------------------------------------------------------
-- S3 Tables (Iceberg) using CTAS for silver_movies
-- Step 1: Create external table (use regular_s3.sql)
-- Step 2: Create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.silver_movies_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/silver_movies/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.silver_movies;


-- S3 TABLES DIRECT
-- ----------------------------------------------------------------------------
-- Direct S3 Tables (Iceberg) creation for silver_movies
CREATE TABLE s3_tables_catalog.database_name.silver_movies_iceberg (
  movie_id BIGINT,
  title STRING,
  title_clean STRING,
  release_year BIGINT,
  content_type STRING,
  genres STRING,
  genres_array STRING,
  genre_count BIGINT,
  is_title_missing BOOLEAN,
  is_genre_missing BOOLEAN,
  has_malformed_year BOOLEAN,
  has_invalid_year BOOLEAN,
  year_pattern_type STRING,
  has_invalid_genre BOOLEAN,
  is_duplicate BOOLEAN,
  duplicate_rank BIGINT,
  loaded_at TIMESTAMP,
  dbt_run_id STRING,
  dbt_run_started_at STRING
)
LOCATION 's3://your-s3-tables-bucket/silver_movies/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.silver_movies_iceberg
SELECT * FROM database_name.silver_movies;

