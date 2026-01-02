-- Direct S3 Tables (Iceberg) creation for silver_links
CREATE TABLE s3_tables_catalog.database_name.silver_links_iceberg (
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
LOCATION 's3://your-s3-tables-bucket/silver_links/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.silver_links_iceberg
SELECT * FROM database_name.silver_links;