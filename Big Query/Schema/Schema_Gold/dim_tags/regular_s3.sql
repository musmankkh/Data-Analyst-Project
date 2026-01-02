-- Regular S3 External Table for dim_tags
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.dim_tags (
  tag_key BIGINT,
  tag_name STRING,
  tag_source STRING,
  tag_length BIGINT,
  word_count BIGINT,
  tag_category STRING,
  tag_sentiment STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/dim_tags/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);