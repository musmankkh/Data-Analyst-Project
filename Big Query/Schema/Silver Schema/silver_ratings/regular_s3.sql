-- Regular S3 External Table for silver_ratings
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.silver_ratings (
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
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/silver/silver_ratings/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);