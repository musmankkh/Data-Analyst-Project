{{config(
    materialized = 'table'
)}}

select 
    user_id,
    user_type,
    tenure_category,

    user_cohort_group,
    rating_behavior_profile,
    taste_diversity,
 
    lifetime_rating_count as total_ratings,
    lifetime_movies_count as unique_movies_rated,
    user_tenure_days as tenure_days,
    




    detailed_avg_rating as avg_rating_given,
    rating_stddev,
    positive_count as positive_ratings_given,
    negative_count as negative_ratings_given,
    neutral_count as neutral_ratings_given,
    pct_positive as pct_positive_ratings,


    user_engagement_score,

     case
        when lifetime_rating_count >= 2000 then 'Top 1% (Power Users)'
        when lifetime_rating_count >= 1000 then 'Top 5%'
        when lifetime_rating_count >= 500 then 'Top 10%'
        when lifetime_rating_count >= 200 then 'Top 25%'
        when lifetime_rating_count >= 100 then 'Top 50%'
        else 'Bottom 50%'
    end as engagement_percentile,

    



    first_rating_date,
    last_rating_date,

    current_timestamp() as kpi_generated_at

from {{ref('view_user_360')}}


