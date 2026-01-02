-- Schema DDL Statements for kpi_time_series
-- Generated: 20251221_140710
-- Source: s3://movielens-elt-project/gold/kpi_time_series/
-- ============================================================================


-- REGULAR S3
-- ----------------------------------------------------------------------------
-- Regular S3 External Table for kpi_time_series
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.kpi_time_series (
  rating_date DATE,
  rating_month TIMESTAMP,
  date_key BIGINT,
  year BIGINT,
  quarter BIGINT,
  month BIGINT,
  month_name STRING,
  week_of_year BIGINT,
  day_of_week_num BIGINT,
  day_name STRING,
  year_month STRING,
  year_quarter STRING,
  is_weekend BOOLEAN,
  is_weekday BOOLEAN,
  season STRING,
  daily_ratings BIGINT,
  daily_active_users BIGINT,
  daily_unique_movies BIGINT,
  daily_avg_rating DECIMAL(38, 9),
  daily_positive_ratings BIGINT,
  daily_negative_ratings BIGINT,
  daily_pct_positive DOUBLE,
  ma_7day_ratings DOUBLE,
  ma_30day_ratings DOUBLE,
  ma_7day_avg_rating DECIMAL(38, 9),
  dod_rating_change BIGINT,
  dod_rating_change_pct DOUBLE,
  wow_rating_change BIGINT,
  wow_rating_change_pct DOUBLE,
  monthly_total_ratings BIGINT,
  monthly_avg_rating DECIMAL(38, 9),
  mom_rating_change BIGINT,
  mom_rating_change_pct DOUBLE,
  quarterly_total_ratings BIGINT,
  quarterly_avg_rating DECIMAL(38, 9),
  qoq_rating_change BIGINT,
  qoq_rating_change_pct DOUBLE,
  trend_direction STRING,
  activity_level STRING,
  kpi_generated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/kpi_time_series/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);


-- S3 TABLES CTAS
-- ----------------------------------------------------------------------------
-- S3 Tables (Iceberg) using CTAS for kpi_time_series
-- Step 1: Create external table (use regular_s3.sql)
-- Step 2: Create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.kpi_time_series_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/kpi_time_series/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.kpi_time_series;


-- S3 TABLES DIRECT
-- ----------------------------------------------------------------------------
-- Direct S3 Tables (Iceberg) creation for kpi_time_series
CREATE TABLE s3_tables_catalog.database_name.kpi_time_series_iceberg (
  rating_date DATE,
  rating_month TIMESTAMP,
  date_key BIGINT,
  year BIGINT,
  quarter BIGINT,
  month BIGINT,
  month_name STRING,
  week_of_year BIGINT,
  day_of_week_num BIGINT,
  day_name STRING,
  year_month STRING,
  year_quarter STRING,
  is_weekend BOOLEAN,
  is_weekday BOOLEAN,
  season STRING,
  daily_ratings BIGINT,
  daily_active_users BIGINT,
  daily_unique_movies BIGINT,
  daily_avg_rating DECIMAL(38, 9),
  daily_positive_ratings BIGINT,
  daily_negative_ratings BIGINT,
  daily_pct_positive DOUBLE,
  ma_7day_ratings DOUBLE,
  ma_30day_ratings DOUBLE,
  ma_7day_avg_rating DECIMAL(38, 9),
  dod_rating_change BIGINT,
  dod_rating_change_pct DOUBLE,
  wow_rating_change BIGINT,
  wow_rating_change_pct DOUBLE,
  monthly_total_ratings BIGINT,
  monthly_avg_rating DECIMAL(38, 9),
  mom_rating_change BIGINT,
  mom_rating_change_pct DOUBLE,
  quarterly_total_ratings BIGINT,
  quarterly_avg_rating DECIMAL(38, 9),
  qoq_rating_change BIGINT,
  qoq_rating_change_pct DOUBLE,
  trend_direction STRING,
  activity_level STRING,
  kpi_generated_at TIMESTAMP
)
LOCATION 's3://your-s3-tables-bucket/kpi_time_series/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.kpi_time_series_iceberg
SELECT * FROM database_name.kpi_time_series;

