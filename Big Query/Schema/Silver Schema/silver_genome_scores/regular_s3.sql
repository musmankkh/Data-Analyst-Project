-- Regular S3 External Table for silver_genome_scores
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.silver_genome_scores (
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
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/silver/silver_genome_scores/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);