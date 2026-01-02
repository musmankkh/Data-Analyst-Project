-- Schema DDL Statements for kpi_user_engagement
-- Generated: 20251221_140710
-- Source: s3://movielens-elt-project/gold/kpi_user_engagement/
-- ============================================================================


-- REGULAR S3
-- ----------------------------------------------------------------------------
-- Regular S3 External Table for kpi_user_engagement
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.kpi_user_engagement (
  user_id BIGINT,
  user_type STRING,
  tenure_category STRING,
  user_cohort_group STRING,
  rating_behavior_profile STRING,
  taste_diversity STRING,
  total_ratings BIGINT,
  unique_movies_rated BIGINT,
  tenure_days BIGINT,
  avg_rating_given DECIMAL(38, 9),
  rating_stddev DOUBLE,
  positive_ratings_given BIGINT,
  negative_ratings_given BIGINT,
  neutral_ratings_given BIGINT,
  pct_positive_ratings DOUBLE,
  user_engagement_score DOUBLE,
  engagement_percentile STRING,
  first_rating_date DATE,
  last_rating_date DATE,
  kpi_generated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/kpi_user_engagement/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);


-- S3 TABLES CTAS
-- ----------------------------------------------------------------------------
-- S3 Tables (Iceberg) using CTAS for kpi_user_engagement
-- Step 1: Create external table (use regular_s3.sql)
-- Step 2: Create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.kpi_user_engagement_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/kpi_user_engagement/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.kpi_user_engagement;


-- S3 TABLES DIRECT
-- ----------------------------------------------------------------------------
-- Direct S3 Tables (Iceberg) creation for kpi_user_engagement
CREATE TABLE s3_tables_catalog.database_name.kpi_user_engagement_iceberg (
  user_id BIGINT,
  user_type STRING,
  tenure_category STRING,
  user_cohort_group STRING,
  rating_behavior_profile STRING,
  taste_diversity STRING,
  total_ratings BIGINT,
  unique_movies_rated BIGINT,
  tenure_days BIGINT,
  avg_rating_given DECIMAL(38, 9),
  rating_stddev DOUBLE,
  positive_ratings_given BIGINT,
  negative_ratings_given BIGINT,
  neutral_ratings_given BIGINT,
  pct_positive_ratings DOUBLE,
  user_engagement_score DOUBLE,
  engagement_percentile STRING,
  first_rating_date DATE,
  last_rating_date DATE,
  kpi_generated_at TIMESTAMP
)
LOCATION 's3://your-s3-tables-bucket/kpi_user_engagement/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.kpi_user_engagement_iceberg
SELECT * FROM database_name.kpi_user_engagement;

