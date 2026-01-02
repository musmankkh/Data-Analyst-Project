
{{ config(
    materialized = 'view'

) }}

with genre_movie_ratings as (
    select
        dm.primary_genre as genre,
        fr.rating_value,
        fr.rating_sentiment,
        fr.user_id,
        fr.movie_id,
        dm.release_year,
        fr.rating_year
    from {{ ref('fact_ratings') }} fr
    left join {{ ref('dim_movies') }} dm using (movie_id)
    where dm.primary_genre is not null
),

genre_aggregates as (
    select
        genre,
        
        
        count(distinct movie_id) as total_movies,
        count(distinct case when release_year >= extract(year from current_date()) - 10
              then movie_id end) as movies_last_10yr,
        count(distinct case when release_year >= extract(year from current_date()) - 5
              then movie_id end) as movies_last_5yr,
        
       
        count(*) as total_ratings,
        round(avg(rating_value), 2) as avg_rating,
        round(stddev(rating_value), 2) as rating_stddev,
        approx_quantiles(rating_value, 100)[offset(50)] as rating_median,
        
       
        countif(rating_sentiment = 'Positive') as positive_ratings,
        countif(rating_sentiment = 'Negative') as negative_ratings,
        round(countif(rating_sentiment = 'Positive') * 100.0 / count(*), 1) as pct_positive,
        
       
        count(distinct user_id) as unique_users,
        round(count(*) * 1.0 / count(distinct movie_id), 1) as avg_ratings_per_movie,
        
       
        count(case when rating_year >= extract(year from current_date()) - 5
              then 1 end) as ratings_last_5yr,
        round(avg(case when rating_year >= extract(year from current_date()) - 5
              then rating_value end), 2) as avg_rating_last_5yr
              
    from genre_movie_ratings
    group by genre
),

genre_with_dimension as (
    select
        ga.*,
        dg.genre_key,
        dg.genre_super_category,
        dg.genre_mood,
        dg.target_audience,
        dg.typical_content_rating,
        dg.typical_setting
    from genre_aggregates ga
    left join {{ ref('dim_genres') }} dg
        on ga.genre = dg.genre_name
),

genre_rankings as (
    select
        *,
        
        
        row_number() over (order by avg_rating desc) as rank_by_rating,
        row_number() over (order by total_ratings desc) as rank_by_volume,
        row_number() over (order by total_movies desc) as rank_by_movie_count,
        row_number() over (order by unique_users desc) as rank_by_user_reach,
        
        
        case
            when avg_rating >= 3.8 then 'High Quality'
            when avg_rating >= 3.5 then 'Good Quality'
            when avg_rating >= 3.0 then 'Average Quality'
            else 'Below Average'
        end as quality_category,
        
        case
            when total_ratings >= 1000000 then 'Mega Popular'
            when total_ratings >= 500000 then 'Very Popular'
            when total_ratings >= 100000 then 'Popular'
            else 'Niche'
        end as popularity_category,
        
        
        case
            when avg_rating_last_5yr > avg_rating + 0.1 then 'Improving'
            when avg_rating_last_5yr < avg_rating - 0.1 then 'Declining'
            else 'Stable'
        end as quality_trend,
        
       
        round(
            (avg_rating / 5.0) * 0.4 +
            (least(log10(total_ratings + 1) / 6, 1.0)) * 0.3 +
            (least(ratings_last_5yr * 1.0 / nullif(total_ratings, 0), 1.0)) * 0.3,
            3
        ) as genre_health_score
        
    from genre_with_dimension
)

select
    genre_key,
    genre as genre_name,
    genre_super_category,
    genre_mood,
    target_audience,
    typical_content_rating,
    typical_setting,
    total_movies,
    movies_last_10yr,
    movies_last_5yr,
    total_ratings,
    avg_rating,
    rating_stddev,
    rating_median,
    positive_ratings,
    negative_ratings,
    pct_positive,
    unique_users,
    avg_ratings_per_movie,
    ratings_last_5yr,
    avg_rating_last_5yr,
    rank_by_rating,
    rank_by_volume,
    rank_by_movie_count,
    rank_by_user_reach,
    quality_category,
    popularity_category,
    quality_trend,
    genre_health_score,
    current_timestamp() as view_generated_at
from genre_rankings
order by total_ratings desc