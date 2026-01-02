-- Direct S3 Tables (Iceberg) creation
CREATE TABLE s3_tables_catalog.database_name.bronze_layer_iceberg (
  movieId BIGINT,
  tagId BIGINT,
  relevance DOUBLE,
  _ingestion_timestamp TIMESTAMP,
  _source_file STRING,
  _source_bucket STRING
)
LOCATION 's3://your-s3-tables-bucket/bronze_layer/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.bronze_layer_iceberg
SELECT * FROM database_name.bronze_layer;