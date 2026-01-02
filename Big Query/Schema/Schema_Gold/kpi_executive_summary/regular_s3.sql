-- Regular S3 External Table for kpi_executive_summary
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.kpi_executive_summary (
  total_users BIGINT,
  total_movies BIGINT,
  total_ratings BIGINT,
  overall_avg_rating DECIMAL(38, 9),
  platform_start_date DATE,
  platform_last_date DATE,
  platform_lifetime_days BIGINT,
  active_users_30d BIGINT,
  active_users_90d BIGINT,
  ratings_30d BIGINT,
  ratings_90d BIGINT,
  avg_rating_30d DECIMAL(38, 9),
  calc_avg_daily_ratings_30d DOUBLE,
  pct_users_active_30d DOUBLE,
  avg_daily_ratings_lifetime DOUBLE,
  avg_ratings_per_user DOUBLE,
  avg_ratings_per_movie DOUBLE,
  super_power_users BIGINT,
  power_users BIGINT,
  heavy_users BIGINT,
  regular_users BIGINT,
  casual_users BIGINT,
  light_users BIGINT,
  high_quality_movies BIGINT,
  good_quality_movies BIGINT,
  average_quality_movies BIGINT,
  blockbluster_movies BIGINT,
  very_popular_movies BIGINT,
  top_10_genres_by_quality LIST<ELEMENT: STRUCT<GENRE_NAME: STRING, AVG_RATING: DECIMAL(38, 9), TOTAL_RATINGS: INT64, GENRE_HEALTH_SCORE: DOUBLE> NOT NULL>,
  top_10_genres_by_volume LIST<ELEMENT: STRUCT<GENRE_NAME: STRING, TOTAL_RATINGS: INT64, AVG_RATING: DECIMAL(38, 9), GENRE_HEALTH_SCORE: DOUBLE> NOT NULL>,
  latest_daily_ratings BIGINT,
  latest_daily_avg_rating DECIMAL(38, 9),
  avg_daily_ratings_30d DOUBLE,
  latest_qoq_change_pct DOUBLE,
  latest_mom_change_pct DOUBLE,
  kpi_generated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/kpi_executive_summary/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);