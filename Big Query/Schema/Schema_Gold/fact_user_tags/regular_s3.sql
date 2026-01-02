-- Regular S3 External Table for fact_user_tags
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.fact_user_tags (
  tag_event_key BIGINT,
  user_id BIGINT,
  movie_id BIGINT,
  tag_key BIGINT,
  date_key BIGINT,
  tag STRING,
  tag_type STRING,
  tag_original STRING,
  tag_length BIGINT,
  tag_word_count BIGINT,
  tagged_year BIGINT,
  tagged_month BIGINT,
  tagged_at TIMESTAMP,
  tagged_date DATE,
  tag_complexity STRING,
  source_loaded_at TIMESTAMP,
  created_at TIMESTAMP,
  source_table STRING
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/fact_user_tags/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);