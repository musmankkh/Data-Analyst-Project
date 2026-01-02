-- Direct S3 Tables (Iceberg) creation for silver_movies
CREATE TABLE s3_tables_catalog.database_name.silver_movies_iceberg (
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
LOCATION 's3://your-s3-tables-bucket/silver_movies/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.silver_movies_iceberg
SELECT * FROM database_name.silver_movies;