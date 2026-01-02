-- Regular S3 External Table for dim_genres
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.dim_genres (
  genre_key BIGINT,
  genre_name STRING,
  genre_super_category STRING,
  genre_mood STRING,
  target_audience STRING,
  typical_content_rating STRING,
  typical_setting STRING,
  created_At TIMESTAMP,
  updated_At TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/dim_genres/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);