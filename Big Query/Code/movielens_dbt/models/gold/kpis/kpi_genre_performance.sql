
{{ config(
    materialized = 'table'
) }}

select
  
    genre_name,
    genre_key,

    genre_super_category,
    genre_mood,
    target_audience,
    typical_content_rating,
    
 
    total_movies,
    movies_last_10yr as movies_recent_10yr,
    movies_last_5yr as movies_recent_5yr,
    total_ratings,
    unique_users as total_unique_users,
    

    avg_rating,
    rating_median,
    rating_stddev,
    pct_positive as pct_positive_ratings,
    
    avg_ratings_per_movie,

    ratings_last_5yr as ratings_recent_5yr,
    avg_rating_last_5yr as avg_rating_recent_5yr,
    
    
    round((ratings_last_5yr * 100.0) / nullif(total_ratings, 0), 1) as pct_ratings_recent_5yr,
    round((movies_last_5yr * 100.0) / nullif(total_movies, 0), 1) as pct_movies_recent_5yr,
    

    rank_by_rating as rank_quality,
    rank_by_volume as rank_popularity,
    rank_by_movie_count as rank_content_volume,
    rank_by_user_reach as rank_audience_reach,
    
 
    quality_category,
    popularity_category,
    quality_trend,
    
    
    genre_health_score,
    

    round(
        total_ratings * 100.0 / 
        (select sum(total_ratings) from {{ ref('view_genre_analysis') }}),
        2
    ) as market_share_pct,
    

    case
        when genre_health_score >= 0.8 then 'Thriving'
        when genre_health_score >= 0.6 then 'Healthy'
        when genre_health_score >= 0.4 then 'Moderate'
        when genre_health_score >= 0.2 then 'Struggling'
        else 'Declining'
    end as health_category,
    
   
    case
        when avg_rating >= 3.7 and total_ratings >= 500000 then 'Stars (High Quality, High Volume)'
        when avg_rating >= 3.7 and total_ratings < 500000 then 'Question Marks (High Quality, Low Volume)'
        when avg_rating < 3.7 and total_ratings >= 500000 then 'Cash Cows (Lower Quality, High Volume)'
        else 'Dogs (Lower Quality, Low Volume)'
    end as strategic_position,
    
    current_timestamp() as kpi_generated_at
    
from {{ ref('view_genre_analysis') }}
order by total_ratings desc