-- Regular S3 External Table for fact_genome_scores
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.fact_genome_scores (
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
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/fact_genome_scores/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);