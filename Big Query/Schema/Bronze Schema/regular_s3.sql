-- Regular S3 External Table
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.bronze_layer (
  movieId BIGINT,
  tagId BIGINT,
  relevance DOUBLE,
  _ingestion_timestamp TIMESTAMP,
  _source_file STRING,
  _source_bucket STRING
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/bronze_layer/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);