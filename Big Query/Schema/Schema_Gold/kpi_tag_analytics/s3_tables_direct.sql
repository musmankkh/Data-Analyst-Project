-- Direct S3 Tables (Iceberg) creation for kpi_tag_analytics
CREATE TABLE s3_tables_catalog.database_name.kpi_tag_analytics_iceberg (
  tag_name STRING,
  tag_key BIGINT,
  tag_category STRING,
  tag_sentiment STRING,
  tag_type STRING,
  user_tag_uses BIGINT,
  users_who_tagged BIGINT,
  unique_movies_tagged BIGINT,
  genome_tag_uses BIGINT,
  genome_avg_relevance DOUBLE,
  genome_movie_count BIGINT,
  high_relevance_count BIGINT,
  avg_rating_tagged_movies DECIMAL(38, 9),
  ratings_for_tagged_movies BIGINT,
  total_movie_coverage BIGINT,
  rank_popularity BIGINT,
  rank_movie_coverage BIGINT,
  rank_genome_relevance BIGINT,
  rank_rating_correlation BIGINT,
  relevance_category STRING,
  activity_status STRING,
  tag_quality_score DOUBLE,
  avg_uses_per_user DOUBLE,
  avg_tags_per_movie DOUBLE,
  pct_uses_last_year DOUBLE,
  pct_uses_last_90d DOUBLE,
  tag_performance_segment STRING,
  tag_lifecycle_stage STRING,
  first_used DATE,
  last_used DATE,
  days_since_last_use BIGINT,
  kpi_generated_at TIMESTAMP
)
LOCATION 's3://your-s3-tables-bucket/kpi_tag_analytics/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.kpi_tag_analytics_iceberg
SELECT * FROM database_name.kpi_tag_analytics;