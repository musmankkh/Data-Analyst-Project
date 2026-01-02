-- Regular S3 External Table for silver_links
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.silver_links (
  movie_id BIGINT,
  imdb_id_raw STRING,
  imdb_id STRING,
  tmdb_id BIGINT,
  is_imdb_missing BOOLEAN,
  is_tmdb_missing BOOLEAN,
  is_duplicate BOOLEAN,
  duplicate_rank BIGINT,
  loaded_at TIMESTAMP,
  dbt_run_id STRING,
  dbt_run_started_at STRING
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/silver/silver_links/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);