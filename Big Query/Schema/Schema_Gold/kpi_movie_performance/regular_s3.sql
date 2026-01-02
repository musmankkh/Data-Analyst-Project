-- Regular S3 External Table for kpi_movie_performance
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.kpi_movie_performance (
  movie_id BIGINT,
  title STRING,
  title_clean STRING,
  release_year BIGINT,
  release_decade STRING,
  movie_era STRING,
  primary_genre STRING,
  genre_category STRING,
  genre_count BIGINT,
  imdb_id STRING,
  tmdb_id BIGINT,
  total_ratings BIGINT,
  avg_rating DECIMAL(38, 9),
  rating_median DECIMAL(38, 9),
  rating_stddev DOUBLE,
  positive_ratings BIGINT,
  negative_ratings BIGINT,
  unique_raters BIGINT,
  pct_positive_ratings DOUBLE,
  pct_negative_ratings DOUBLE,
  total_user_tags BIGINT,
  unique_taggers BIGINT,
  total_genome_tags BIGINT,
  high_relevance_tags BIGINT,
  movie_performance_tier STRING,
  popularity_tier STRING,
  controversy_level STRING,
  rating_tier STRING,
  composite_score DOUBLE,
  first_rating_date DATE,
  last_rating_date DATE,
  rating_span_days BIGINT,
  data_completeness STRING,
  kpi_generated_at TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://movielens-elt-project/gold/kpi_movie_performance/'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);