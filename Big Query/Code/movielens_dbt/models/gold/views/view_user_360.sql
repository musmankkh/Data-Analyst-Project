
{{ config(
    materialized = 'view'
) }}

with user_rating_details as (
    select
        fr.user_id,
        count(*) as detailed_rating_count,
        round(avg(fr.rating_value), 2) as detailed_avg_rating,
        round(stddev(fr.rating_value), 2) as rating_stddev,
        countif(fr.rating_sentiment = 'Positive') as positive_count,
        countif(fr.rating_sentiment = 'Negative') as negative_count,
        countif(fr.rating_sentiment = 'Neutral') as neutral_count,
        round(countif(fr.rating_sentiment = 'Positive') * 100.0 / count(*), 1) as pct_positive,
        
        -- Recent activity
        countif(fr.rating_date >= date_sub(current_date(), interval 30 day)) as ratings_last_30d,
        countif(fr.rating_date >= date_sub(current_date(), interval 90 day)) as ratings_last_90d,
        countif(fr.rating_date >= date_sub(current_date(), interval 365 day)) as ratings_last_year
    from {{ ref('fact_ratings') }} fr
    group by fr.user_id
),

-- First aggregate by user and genre
user_genre_stats as (
    select
        fr.user_id,
        dm.primary_genre,
        count(*) as rating_count,
        round(avg(fr.rating_value), 2) as avg_rating
    from {{ ref('fact_ratings') }} fr
    left join {{ ref('dim_movies') }} dm using (movie_id)
    where dm.primary_genre is not null
    group by fr.user_id, dm.primary_genre
),

-- Then create the array from the pre-aggregated data
user_genre_preferences as (
    select
        user_id,
        array_agg(
            struct(
                primary_genre as genre,
                rating_count,
                avg_rating
            )
            order by rating_count desc
            limit 5
        ) as top_genres
    from user_genre_stats
    group by user_id
),

user_tagging_activity as (
    select
        user_id,
        count(*) as total_tags_applied,
        count(distinct movie_id) as movies_tagged,
        count(distinct tag) as unique_tags_used,
        array_agg(distinct tag order by tag limit 10) as favorite_tags
    from {{ ref('fact_user_tags') }}
    group by user_id
)

select
    -- User Dimension
    du.user_id,
    du.first_rating_date,
    du.last_rating_date,
    du.user_tenure_days,
  
    du.lifetime_rating_count,
    du.lifetime_movies_count,
    du.avg_ratings_per_day,
    du.user_type,
    du.tenure_category,
    du.activity_status,
    du.user_cohort,
    du.user_cohort_group,
    
    -- Detailed Rating Metrics
    urd.detailed_rating_count,
    urd.detailed_avg_rating,
    urd.rating_stddev,
    urd.positive_count,
    urd.negative_count,
    urd.neutral_count,
    urd.pct_positive,
    urd.ratings_last_30d,
    urd.ratings_last_90d,
    urd.ratings_last_year,
    
    -- Genre Preferences
    ugp.top_genres,
    
    -- Tagging Activity
    coalesce(uta.total_tags_applied, 0) as total_tags_applied,
    coalesce(uta.movies_tagged, 0) as movies_tagged,
    coalesce(uta.unique_tags_used, 0) as unique_tags_used,
    uta.favorite_tags,
    
    -- Derived Classifications
    case
        when urd.detailed_avg_rating >= 4.0 then 'Generous Rater'
        when urd.detailed_avg_rating >= 3.5 then 'Positive Rater'
        when urd.detailed_avg_rating >= 3.0 then 'Balanced Rater'
        when urd.detailed_avg_rating >= 2.5 then 'Critical Rater'
        else 'Harsh Critic'
    end as rating_behavior_profile,
    
    case
        when urd.rating_stddev >= 1.5 then 'Highly Diverse Tastes'
        when urd.rating_stddev >= 1.0 then 'Diverse Tastes'
        else 'Consistent Tastes'
    end as taste_diversity,
    
    -- Engagement Score
    round(
        (least(log10(du.lifetime_rating_count + 1) / 3, 1.0)) * 0.4 +
        (case when urd.ratings_last_year > 0 then 1.0
              when urd.ratings_last_90d > 0 then 0.5
              else 0.0 end) * 0.3 +
        (least(du.user_tenure_days / 3650.0, 1.0)) * 0.3,
        3
    ) as user_engagement_score,
    
    current_timestamp() as view_generated_at
    
from {{ ref('dim_users') }} du
left join user_rating_details urd using (user_id)
left join user_genre_preferences ugp using (user_id)
left join user_tagging_activity uta using (user_id)