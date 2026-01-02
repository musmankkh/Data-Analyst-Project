-- Direct S3 Tables (Iceberg) creation for dim_tags
CREATE TABLE s3_tables_catalog.database_name.dim_tags_iceberg (
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
LOCATION 's3://your-s3-tables-bucket/dim_tags/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.dim_tags_iceberg
SELECT * FROM database_name.dim_tags;