{{config(
    materialized = 'table'
)}}

with user_first_last as (
    select 
        user_id,
        min(rating_date) as first_rating_date,
        max(rating_date) as last_rating_date,
        min(rating_year) as first_rating_year,
        max(rating_year) as last_rating_year,
        count(*) as lifetime_rating_count,
        count(distinct movie_id) as lifetime_movies_count
    from {{ ref('silver_ratings') }}
    group by user_id
),

user_dimension as (
    select
        user_id,
        first_rating_date,
        last_rating_date,
        first_rating_year,
        last_rating_year,
        date_diff(last_rating_date, first_rating_date, day) as user_tenure_days,
        date_diff(current_date(), last_rating_date, day) as day_since_last_activity,

        lifetime_rating_count,
        lifetime_movies_count,

        case 
            when lifetime_rating_count >= 2000 then "Super Power User"
            when lifetime_rating_count >= 1000 then "Power User"
            when lifetime_rating_count >= 500 then "Heavy User"
            when lifetime_rating_count >= 100 then " Regular User"
            when lifetime_rating_count >= 20 then "Casual User"
            else "Light User"
        end as user_type,

        case
            when date_diff(last_rating_date, first_rating_date, day) >= 3650 then "10+ Years"
            when date_diff(last_rating_date, first_rating_date, day) >= 1825 then "5-10 Years"
            when date_diff(last_rating_date, first_rating_date, day) >= 730 then "2-5 Years" 
            when date_diff(last_rating_date, first_rating_date, day) >= 100 then "1-2 Years"
            else "Less than 1 Year"
        end as tenure_category,

        case
            when date_diff(current_date(), last_rating_date, day) <= 30 then "Active (30d)"
            when date_diff(current_date(), last_rating_date, day) <= 90 then "Recent (90d)"
            when date_diff(current_date(), last_rating_date, day) <= 365 then "Occasional (1yr)"
            when date_diff(current_date(), last_rating_date, day) <= 730 then "Dormant (2yr)"
            else "Inactive (2+yr)"
        end  as activity_status,

        concat('Cohort ', cast(first_rating_year as string)) as user_cohort,
        
        case
            when first_rating_year >= 2020 then '2020s Cohort'
            when first_rating_year >= 2015 then '2015-2019 Cohort'
            when first_rating_year >= 2010 then '2010-2014 Cohort'
            when first_rating_year >= 2005 then '2005-2009 Cohort'
            else 'Pre-2005 Cohort'
        end as user_cohort_group,


        round(
            lifetime_rating_count * 1.0 / 
            nullif(date_diff(last_rating_date, first_rating_date, day), 0), 2
        ) as avg_ratings_per_day,

        current_timestamp() as effective_date,
        cast('9999-12-31' as timestamp) as expiration_date,
        true as is_current,
        current_timestamp() as created_at

    from user_first_last    

)

select 
    user_id,
    first_rating_date,
    last_rating_date,
    first_rating_year,
    last_rating_year,
    user_tenure_days,
    day_since_last_activity,

    lifetime_rating_count,
    lifetime_movies_count,
    avg_ratings_per_day,

    user_type,
    tenure_category,
    activity_status,
    user_cohort,
    user_cohort_group,

    effective_date,
    expiration_date,
    is_current,
    created_at

from user_dimension
