-- Regular S3 External Table for kpi_genre_combinations
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.kpi_genre_combinations (
  genre_combination STRING,
  primary_genre STRING,
  secondary_genre STRING,
  primary_super_category STRING,
  secondary_super_category STRING,
  total_movies BIGINT,
  recent_movies_10yr BIGINT,
  recent_movies_5yr BIGINT,
  total_ratings BIGINT,
  unique_users BIGINT,
  avg_rating DECIMAL(38, 9),
  rating_median DECIMAL(38, 9),
  rating_stddev DOUBLE,
  positive_ratings BIGINT,
  negative_ratings BIGINT,
  pct_positive DOUBLE,
  avg_ratings_per_movie DOUBLE,
  combination_tier STRING,
  synergy_level STRING,
  market_position STRING,
  genre_synergy_score DOUBLE,
  market_share_pct DOUBLE,
  pct_recent_movies DOUBLE,
  trend_status STRING,
  audience_reception STRING,
  kpi_generated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/kpi_genre_combinations/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);