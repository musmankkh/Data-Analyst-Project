-- Direct S3 Tables (Iceberg) creation for silver_genome_scores
CREATE TABLE s3_tables_catalog.database_name.silver_genome_scores_iceberg (
  movie_id BIGINT,
  tag_id BIGINT,
  relevance DOUBLE,
  relevance_rounded DOUBLE,
  relevance_category STRING,
  is_relevance_out_of_range BOOLEAN,
  is_duplicate BOOLEAN,
  duplicate_rank BIGINT,
  loaded_at TIMESTAMP,
  dbt_run_id STRING,
  dbt_run_started_at STRING
)
LOCATION 's3://your-s3-tables-bucket/silver_genome_scores/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.silver_genome_scores_iceberg
SELECT * FROM database_name.silver_genome_scores;