-- Regular S3 External Table for silver_tags
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.silver_tags (
  user_id BIGINT,
  movie_id BIGINT,
  tag STRING,
  tag_original STRING,
  tag_length BIGINT,
  tag_word_count BIGINT,
  tag_type STRING,
  tagged_at TIMESTAMP,
  tagged_date DATE,
  tagged_year BIGINT,
  tagged_month BIGINT,
  tagged_day_of_week BIGINT,
  is_tag_missing BOOLEAN,
  is_tag_too_short BOOLEAN,
  is_tag_too_long BOOLEAN,
  has_special_chars BOOLEAN,
  is_duplicate BOOLEAN,
  duplicate_rank BIGINT,
  loaded_at TIMESTAMP,
  dbt_run_id STRING,
  dbt_run_started_at STRING
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/silver/silver_tags/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);