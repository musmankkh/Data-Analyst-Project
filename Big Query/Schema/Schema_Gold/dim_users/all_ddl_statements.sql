-- Schema DDL Statements for dim_users
-- Generated: 20251221_140710
-- Source: s3://movielens-elt-project/gold/dim_users/
-- ============================================================================


-- REGULAR S3
-- ----------------------------------------------------------------------------
-- Regular S3 External Table for dim_users
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.dim_users (
  user_id BIGINT,
  first_rating_date DATE,
  last_rating_date DATE,
  first_rating_year BIGINT,
  last_rating_year BIGINT,
  user_tenure_days BIGINT,
  day_since_last_activity BIGINT,
  lifetime_rating_count BIGINT,
  lifetime_movies_count BIGINT,
  avg_ratings_per_day DOUBLE,
  user_type STRING,
  tenure_category STRING,
  activity_status STRING,
  user_cohort STRING,
  user_cohort_group STRING,
  effective_date TIMESTAMP,
  expiration_date TIMESTAMP,
  is_current BOOLEAN,
  created_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/dim_users/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);


-- S3 TABLES CTAS
-- ----------------------------------------------------------------------------
-- S3 Tables (Iceberg) using CTAS for dim_users
-- Step 1: Create external table (use regular_s3.sql)
-- Step 2: Create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.dim_users_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/dim_users/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.dim_users;


-- S3 TABLES DIRECT
-- ----------------------------------------------------------------------------
-- Direct S3 Tables (Iceberg) creation for dim_users
CREATE TABLE s3_tables_catalog.database_name.dim_users_iceberg (
  user_id BIGINT,
  first_rating_date DATE,
  last_rating_date DATE,
  first_rating_year BIGINT,
  last_rating_year BIGINT,
  user_tenure_days BIGINT,
  day_since_last_activity BIGINT,
  lifetime_rating_count BIGINT,
  lifetime_movies_count BIGINT,
  avg_ratings_per_day DOUBLE,
  user_type STRING,
  tenure_category STRING,
  activity_status STRING,
  user_cohort STRING,
  user_cohort_group STRING,
  effective_date TIMESTAMP,
  expiration_date TIMESTAMP,
  is_current BOOLEAN,
  created_at TIMESTAMP
)
LOCATION 's3://your-s3-tables-bucket/dim_users/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.dim_users_iceberg
SELECT * FROM database_name.dim_users;

