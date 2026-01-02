-- Direct S3 Tables (Iceberg) creation for silver_tags
CREATE TABLE s3_tables_catalog.database_name.silver_tags_iceberg (
  user_id BIGINT,
  movie_id BIGINT,
  tag STRING,
  tag_original STRING,
  tag_length BIGINT,
  tag_word_count BIGINT,
  tag_type STRING,
  tagged_at TIMESTAMP,
  tagged_date DATE,
  tagged_year BIGINT,
  tagged_month BIGINT,
  tagged_day_of_week BIGINT,
  is_tag_missing BOOLEAN,
  is_tag_too_short BOOLEAN,
  is_tag_too_long BOOLEAN,
  has_special_chars BOOLEAN,
  is_duplicate BOOLEAN,
  duplicate_rank BIGINT,
  loaded_at TIMESTAMP,
  dbt_run_id STRING,
  dbt_run_started_at STRING
)
LOCATION 's3://your-s3-tables-bucket/silver_tags/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.silver_tags_iceberg
SELECT * FROM database_name.silver_tags;