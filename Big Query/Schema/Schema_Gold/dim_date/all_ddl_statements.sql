-- Schema DDL Statements for dim_date
-- Generated: 20251221_140710
-- Source: s3://movielens-elt-project/gold/dim_date/
-- ============================================================================


-- REGULAR S3
-- ----------------------------------------------------------------------------
-- Regular S3 External Table for dim_date
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.dim_date (
  date_key BIGINT,
  date_day DATE,
  year BIGINT,
  month BIGINT,
  quarter BIGINT,
  week_of_year BIGINT,
  day_of_month BIGINT,
  day_of_week_num BIGINT,
  day_of_year BIGINT,
  year_month STRING,
  year_quarter STRING,
  month_name STRING,
  day_name STRING,
  month_name_short STRING,
  day_name_short STRING,
  week_start_date DATE,
  month_start_date DATE,
  quarter_start_date DATE,
  year_start_date DATE,
  month_end_date DATE,
  quarter_end_date DATE,
  year_end_date DATE,
  days_from_today BIGINT,
  weeks_from_today BIGINT,
  months_from_today BIGINT,
  is_weekend BOOLEAN,
  is_weekday BOOLEAN,
  is_month_start BOOLEAN,
  is_month_end BOOLEAN,
  is_quarter_start BOOLEAN,
  is_quarter_end BOOLEAN,
  is_year_start BOOLEAN,
  is_year_end BOOLEAN,
  is_today BOOLEAN,
  is_yesterday BOOLEAN,
  period_label STRING,
  season STRING,
  fiscal_quarter BIGINT,
  fiscal_year BIGINT,
  created_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/dim_date/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);


-- S3 TABLES CTAS
-- ----------------------------------------------------------------------------
-- S3 Tables (Iceberg) using CTAS for dim_date
-- Step 1: Create external table (use regular_s3.sql)
-- Step 2: Create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.dim_date_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/dim_date/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.dim_date;


-- S3 TABLES DIRECT
-- ----------------------------------------------------------------------------
-- Direct S3 Tables (Iceberg) creation for dim_date
CREATE TABLE s3_tables_catalog.database_name.dim_date_iceberg (
  date_key BIGINT,
  date_day DATE,
  year BIGINT,
  month BIGINT,
  quarter BIGINT,
  week_of_year BIGINT,
  day_of_month BIGINT,
  day_of_week_num BIGINT,
  day_of_year BIGINT,
  year_month STRING,
  year_quarter STRING,
  month_name STRING,
  day_name STRING,
  month_name_short STRING,
  day_name_short STRING,
  week_start_date DATE,
  month_start_date DATE,
  quarter_start_date DATE,
  year_start_date DATE,
  month_end_date DATE,
  quarter_end_date DATE,
  year_end_date DATE,
  days_from_today BIGINT,
  weeks_from_today BIGINT,
  months_from_today BIGINT,
  is_weekend BOOLEAN,
  is_weekday BOOLEAN,
  is_month_start BOOLEAN,
  is_month_end BOOLEAN,
  is_quarter_start BOOLEAN,
  is_quarter_end BOOLEAN,
  is_year_start BOOLEAN,
  is_year_end BOOLEAN,
  is_today BOOLEAN,
  is_yesterday BOOLEAN,
  period_label STRING,
  season STRING,
  fiscal_quarter BIGINT,
  fiscal_year BIGINT,
  created_at TIMESTAMP
)
LOCATION 's3://your-s3-tables-bucket/dim_date/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.dim_date_iceberg
SELECT * FROM database_name.dim_date;

