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