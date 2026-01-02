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