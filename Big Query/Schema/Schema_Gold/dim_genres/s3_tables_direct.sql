-- Direct S3 Tables (Iceberg) creation for dim_genres
CREATE TABLE s3_tables_catalog.database_name.dim_genres_iceberg (
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
LOCATION 's3://your-s3-tables-bucket/dim_genres/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.dim_genres_iceberg
SELECT * FROM database_name.dim_genres;