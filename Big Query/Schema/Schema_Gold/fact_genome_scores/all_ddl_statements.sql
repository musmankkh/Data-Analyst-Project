-- Schema DDL Statements for fact_genome_scores
-- Generated: 20251221_140710
-- Source: s3://movielens-elt-project/gold/fact_genome_scores/
-- ============================================================================


-- REGULAR S3
-- ----------------------------------------------------------------------------
-- Regular S3 External Table for fact_genome_scores
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.fact_genome_scores (
  genome_score_key BIGINT,
  movie_id BIGINT,
  tag_id BIGINT,
  tag_key BIGINT,
  tag STRING,
  tag_original STRING,
  relevance_score DOUBLE,
  relevance_rounded DOUBLE,
  relevance_category STRING,
  relevance_detail_category STRING,
  relevance_band DOUBLE,
  is_highly_relevant BIGINT,
  is_medium_relevant BIGINT,
  is_lowly_relevant BIGINT,
  source_loaded_at TIMESTAMP,
  created_at TIMESTAMP,
  source_table STRING
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/fact_genome_scores/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);


-- S3 TABLES CTAS
-- ----------------------------------------------------------------------------
-- S3 Tables (Iceberg) using CTAS for fact_genome_scores
-- Step 1: Create external table (use regular_s3.sql)
-- Step 2: Create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.fact_genome_scores_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/fact_genome_scores/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.fact_genome_scores;


-- S3 TABLES DIRECT
-- ----------------------------------------------------------------------------
-- Direct S3 Tables (Iceberg) creation for fact_genome_scores
CREATE TABLE s3_tables_catalog.database_name.fact_genome_scores_iceberg (
  genome_score_key BIGINT,
  movie_id BIGINT,
  tag_id BIGINT,
  tag_key BIGINT,
  tag STRING,
  tag_original STRING,
  relevance_score DOUBLE,
  relevance_rounded DOUBLE,
  relevance_category STRING,
  relevance_detail_category STRING,
  relevance_band DOUBLE,
  is_highly_relevant BIGINT,
  is_medium_relevant BIGINT,
  is_lowly_relevant BIGINT,
  source_loaded_at TIMESTAMP,
  created_at TIMESTAMP,
  source_table STRING
)
LOCATION 's3://your-s3-tables-bucket/fact_genome_scores/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.fact_genome_scores_iceberg
SELECT * FROM database_name.fact_genome_scores;

