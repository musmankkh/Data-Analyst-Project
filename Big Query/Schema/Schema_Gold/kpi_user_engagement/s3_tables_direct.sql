-- Direct S3 Tables (Iceberg) creation for kpi_user_engagement
CREATE TABLE s3_tables_catalog.database_name.kpi_user_engagement_iceberg (
  user_id BIGINT,
  user_type STRING,
  tenure_category STRING,
  user_cohort_group STRING,
  rating_behavior_profile STRING,
  taste_diversity STRING,
  total_ratings BIGINT,
  unique_movies_rated BIGINT,
  tenure_days BIGINT,
  avg_rating_given DECIMAL(38, 9),
  rating_stddev DOUBLE,
  positive_ratings_given BIGINT,
  negative_ratings_given BIGINT,
  neutral_ratings_given BIGINT,
  pct_positive_ratings DOUBLE,
  user_engagement_score DOUBLE,
  engagement_percentile STRING,
  first_rating_date DATE,
  last_rating_date DATE,
  kpi_generated_at TIMESTAMP
)
LOCATION 's3://your-s3-tables-bucket/kpi_user_engagement/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.kpi_user_engagement_iceberg
SELECT * FROM database_name.kpi_user_engagement;