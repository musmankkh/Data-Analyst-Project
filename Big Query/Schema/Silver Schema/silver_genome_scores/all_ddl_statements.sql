-- Schema DDL Statements for silver_genome_scores
-- Generated: 20251221_132625
-- Source: s3://movielens-elt-project/silver/silver_genome_scores/
-- ============================================================================


-- REGULAR S3
-- ----------------------------------------------------------------------------
-- Regular S3 External Table for silver_genome_scores
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.silver_genome_scores (
  movie_id BIGINT,
  tag_id BIGINT,
  relevance DOUBLE,
  relevance_rounded DOUBLE,
  relevance_category STRING,
  is_relevance_out_of_range BOOLEAN,
  is_duplicate BOOLEAN,
  duplicate_rank BIGINT,
  loaded_at TIMESTAMP,
  dbt_run_id STRING,
  dbt_run_started_at STRING
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/silver/silver_genome_scores/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);


-- S3 TABLES CTAS
-- ----------------------------------------------------------------------------
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


-- S3 TABLES DIRECT
-- ----------------------------------------------------------------------------
-- Direct S3 Tables (Iceberg) creation for silver_genome_scores
CREATE TABLE s3_tables_catalog.database_name.silver_genome_scores_iceberg (
  movie_id BIGINT,
  tag_id BIGINT,
  relevance DOUBLE,
  relevance_rounded DOUBLE,
  relevance_category STRING,
  is_relevance_out_of_range BOOLEAN,
  is_duplicate BOOLEAN,
  duplicate_rank BIGINT,
  loaded_at TIMESTAMP,
  dbt_run_id STRING,
  dbt_run_started_at STRING
)
LOCATION 's3://your-s3-tables-bucket/silver_genome_scores/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.silver_genome_scores_iceberg
SELECT * FROM database_name.silver_genome_scores;

