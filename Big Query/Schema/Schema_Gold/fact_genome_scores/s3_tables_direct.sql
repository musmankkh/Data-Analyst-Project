-- Direct S3 Tables (Iceberg) creation for fact_genome_scores
CREATE TABLE s3_tables_catalog.database_name.fact_genome_scores_iceberg (
  genome_score_key BIGINT,
  movie_id BIGINT,
  tag_id BIGINT,
  tag_key BIGINT,
  tag STRING,
  tag_original STRING,
  relevance_score DOUBLE,
  relevance_rounded DOUBLE,
  relevance_category STRING,
  relevance_detail_category STRING,
  relevance_band DOUBLE,
  is_highly_relevant BIGINT,
  is_medium_relevant BIGINT,
  is_lowly_relevant BIGINT,
  source_loaded_at TIMESTAMP,
  created_at TIMESTAMP,
  source_table STRING
)
LOCATION 's3://your-s3-tables-bucket/fact_genome_scores/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.fact_genome_scores_iceberg
SELECT * FROM database_name.fact_genome_scores;