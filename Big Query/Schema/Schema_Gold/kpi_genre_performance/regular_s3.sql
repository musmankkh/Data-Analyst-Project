-- Regular S3 External Table for kpi_genre_performance
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.kpi_genre_performance (
  genre_name STRING,
  genre_key BIGINT,
  genre_super_category STRING,
  genre_mood STRING,
  target_audience STRING,
  typical_content_rating STRING,
  total_movies BIGINT,
  movies_recent_10yr BIGINT,
  movies_recent_5yr BIGINT,
  total_ratings BIGINT,
  total_unique_users BIGINT,
  avg_rating DECIMAL(38, 9),
  rating_median DECIMAL(38, 9),
  rating_stddev DOUBLE,
  pct_positive_ratings DOUBLE,
  avg_ratings_per_movie DOUBLE,
  ratings_recent_5yr BIGINT,
  avg_rating_recent_5yr DECIMAL(38, 9),
  pct_ratings_recent_5yr DOUBLE,
  pct_movies_recent_5yr DOUBLE,
  rank_quality BIGINT,
  rank_popularity BIGINT,
  rank_content_volume BIGINT,
  rank_audience_reach BIGINT,
  quality_category STRING,
  popularity_category STRING,
  quality_trend STRING,
  genre_health_score DOUBLE,
  market_share_pct DOUBLE,
  health_category STRING,
  strategic_position STRING,
  kpi_generated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/kpi_genre_performance/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);