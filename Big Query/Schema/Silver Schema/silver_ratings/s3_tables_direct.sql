-- Direct S3 Tables (Iceberg) creation for silver_ratings
CREATE TABLE s3_tables_catalog.database_name.silver_ratings_iceberg (
  user_id BIGINT,
  movie_id BIGINT,
  rating DECIMAL(38, 9),
  timestamp_unix BIGINT,
  rating_datetime TIMESTAMP,
  rating_date DATE,
  rating_year BIGINT,
  rating_month BIGINT,
  rating_day BIGINT,
  rating_day_of_week STRING,
  data_quality_status STRING,
  duplicate_rank BIGINT,
  loaded_at TIMESTAMP,
  dbt_run_id STRING,
  dbt_run_started_at STRING
)
LOCATION 's3://your-s3-tables-bucket/silver_ratings/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.silver_ratings_iceberg
SELECT * FROM database_name.silver_ratings;