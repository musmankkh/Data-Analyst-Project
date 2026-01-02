{{ config(
    materialized = 'table'
) }}

with movie_genres_expanded as (
    select
        dm.movie_id,
        dm.title_clean,
        dm.release_year,
        g1 as genre_1,
        g2 as genre_2
    from {{ ref('dim_movies') }} dm
    cross join unnest(dm.genres_array) as g1 with offset as pos1
    cross join unnest(dm.genres_array) as g2 with offset as pos2
    where pos1 < pos2 
      and array_length(dm.genres_array) >= 2  
),

genre_pair_ratings as (
    select
        mge.genre_1,
        mge.genre_2,
        mge.movie_id,
        fr.rating_value,
        fr.rating_sentiment,
        fr.user_id,
        mge.release_year
    from movie_genres_expanded mge
    left join {{ ref('fact_ratings') }} fr
        on mge.movie_id = fr.movie_id
),

genre_combination_metrics as (
    select
        case
            when genre_1 < genre_2 then concat(genre_1, ' + ', genre_2)
            else concat(genre_2, ' + ', genre_1)
        end as genre_combination,
        
        case
            when genre_1 < genre_2 then genre_1
            else genre_2
        end as primary_genre,
        
        case
            when genre_1 < genre_2 then genre_2
            else genre_1
        end as secondary_genre,
        
        
        count(distinct movie_id) as total_movies,
        count(distinct case when release_year >= extract(year from current_date()) - 10
              then movie_id end) as recent_movies_10yr,
        count(distinct case when release_year >= extract(year from current_date()) - 5
              then movie_id end) as recent_movies_5yr,
        
       
        count(rating_value) as total_ratings,
        round(avg(rating_value), 2) as avg_rating,
        round(stddev(rating_value), 2) as rating_stddev,
        approx_quantiles(rating_value, 100)[offset(50)] as rating_median,
        
        
        countif(rating_sentiment = 'Positive') as positive_ratings,
        countif(rating_sentiment = 'Negative') as negative_ratings,
        round(countif(rating_sentiment = 'Positive') * 100.0 / count(rating_value), 1) as pct_positive,
        
      
        count(distinct user_id) as unique_users,
        round(count(rating_value) * 1.0 / count(distinct movie_id), 1) as avg_ratings_per_movie
        
    from genre_pair_ratings
    where rating_value is not null
    group by 1,2,3
    having count(rating_value) >= 50  
),

genre_combo_enriched as (
    select
        gcm.*,
        
        
        dg1.genre_super_category as primary_super_category,
        dg2.genre_super_category as secondary_super_category,
        
        
        row_number() over (order by avg_rating desc) as rank_by_rating,
        row_number() over (order by total_ratings desc) as rank_by_volume,
        row_number() over (order by total_movies desc) as rank_by_movie_count,
        
        
        case
            when avg_rating >= 4.0 and total_ratings >= 10000 then 'Premium Combo (High Quality, High Volume)'
            when avg_rating >= 4.0 and total_ratings >= 1000 then 'High Quality Combo'
            when avg_rating >= 3.7 and total_ratings >= 10000 then 'Popular & Good Combo'
            when avg_rating >= 3.7 then 'Good Quality Combo'
            when total_ratings >= 10000 then 'Very Popular Combo'
            when total_ratings >= 1000 then 'Popular Combo'
            else 'Niche Combo'
        end as combination_tier,
        
        
        case
            when avg_rating >= 3.8 then 'Strong Synergy'
            when avg_rating >= 3.5 then 'Good Synergy'
            when avg_rating >= 3.2 then 'Moderate Synergy'
            else 'Weak Synergy'
        end as synergy_level,
        
        round(
            (avg_rating / 5.0) * 0.5 +
            (least(log10(total_ratings + 1) / 5, 1.0)) * 0.3 +
            (least(total_movies / 50.0, 1.0)) * 0.2,
            3
        ) as genre_synergy_score,
        
        
        case
            when avg_rating >= 3.8 and total_ratings >= 5000 then 'Market Leader'
            when avg_rating >= 3.8 then 'Quality Niche'
            when total_ratings >= 5000 then 'Mass Market'
            else 'Emerging'
        end as market_position
        
    from genre_combination_metrics gcm
    left join {{ ref('dim_genres') }} dg1
        on gcm.primary_genre = dg1.genre_name
    left join {{ ref('dim_genres') }} dg2
        on gcm.secondary_genre = dg2.genre_name
)

select
  
    genre_combination,
    primary_genre,
    secondary_genre,
    primary_super_category,
    secondary_super_category,
    
    
    total_movies,
    recent_movies_10yr,
    recent_movies_5yr,
    total_ratings,
    unique_users,
    

    avg_rating,
    rating_median,
    rating_stddev,
    positive_ratings,
    negative_ratings,
    pct_positive,
    avg_ratings_per_movie,

    combination_tier,
    synergy_level,
    market_position,
    

    genre_synergy_score,
    
  
    round(total_ratings * 100.0 / 
        (select sum(total_ratings) from genre_combination_metrics), 2) as market_share_pct,
    
    round(recent_movies_5yr * 100.0 / nullif(total_movies, 0), 1) as pct_recent_movies,
    
   
    case
        when recent_movies_5yr >= total_movies * 0.5 then 'Trending Up'
        when recent_movies_5yr >= total_movies * 0.3 then 'Stable'
        else 'Declining'
    end as trend_status,
    
    case
        when pct_positive >= 70 then 'Highly Favorable'
        when pct_positive >= 60 then 'Favorable'
        when pct_positive >= 50 then 'Mixed'
        else 'Challenging'
    end as audience_reception,
    

    current_timestamp() as kpi_generated_at
    
from genre_combo_enriched
order by total_ratings desc