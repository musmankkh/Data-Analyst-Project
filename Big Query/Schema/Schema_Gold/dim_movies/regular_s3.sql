-- Regular S3 External Table for dim_movies
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.dim_movies (
  movie_id BIGINT,
  title STRING,
  title_clean STRING,
  release_year BIGINT,
  release_decade DOUBLE,
  release_decade_label STRING,
  movie_era STRING,
  movie_age_years BIGINT,
  movie_age_category STRING,
  genres STRING,
  genres_array STRING,
  genre_count BIGINT,
  primary_genre STRING,
  genre_category STRING,
  imdb_id STRING,
  tmdb_id BIGINT,
  is_genre_missing BOOLEAN,
  has_invalid_year BOOLEAN,
  has_invalid_genre BOOLEAN,
  is_imdb_missing BOOLEAN,
  is_tmdb_missing BOOLEAN,
  data_completeness STRING,
  effective_date TIMESTAMP,
  expiration_date TIMESTAMP,
  is_current BOOLEAN,
  created_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/dim_movies/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);