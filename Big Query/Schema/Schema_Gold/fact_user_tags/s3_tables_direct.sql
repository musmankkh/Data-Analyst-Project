-- Direct S3 Tables (Iceberg) creation for fact_user_tags
CREATE TABLE s3_tables_catalog.database_name.fact_user_tags_iceberg (
  tag_event_key BIGINT,
  user_id BIGINT,
  movie_id BIGINT,
  tag_key BIGINT,
  date_key BIGINT,
  tag STRING,
  tag_type STRING,
  tag_original STRING,
  tag_length BIGINT,
  tag_word_count BIGINT,
  tagged_year BIGINT,
  tagged_month BIGINT,
  tagged_at TIMESTAMP,
  tagged_date DATE,
  tag_complexity STRING,
  source_loaded_at TIMESTAMP,
  created_at TIMESTAMP,
  source_table STRING
)
LOCATION 's3://your-s3-tables-bucket/fact_user_tags/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.fact_user_tags_iceberg
SELECT * FROM database_name.fact_user_tags;