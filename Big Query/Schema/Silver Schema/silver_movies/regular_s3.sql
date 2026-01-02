-- Regular S3 External Table for silver_movies
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.silver_movies (
  movie_id BIGINT,
  title STRING,
  title_clean STRING,
  release_year BIGINT,
  content_type STRING,
  genres STRING,
  genres_array STRING,
  genre_count BIGINT,
  is_title_missing BOOLEAN,
  is_genre_missing BOOLEAN,
  has_malformed_year BOOLEAN,
  has_invalid_year BOOLEAN,
  year_pattern_type STRING,
  has_invalid_genre BOOLEAN,
  is_duplicate BOOLEAN,
  duplicate_rank BIGINT,
  loaded_at TIMESTAMP,
  dbt_run_id STRING,
  dbt_run_started_at STRING
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/silver/silver_movies/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);