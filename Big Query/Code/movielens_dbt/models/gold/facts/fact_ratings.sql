{{
    config(
        materialized = 'table',
        partition_by = {
            "field": "rating_date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by = [
            "user_id",
            "movie_id"
        ]
    )    
}}

with rating_base as(
    select
        user_id,
        movie_id,
        rating,
        rating_date,
        rating_datetime,
        rating_year,
        rating_month,
        rating_day,
        rating_day_of_week,
        timestamp_unix,
        loaded_at
    from {{ref('silver_ratings')}}    

),

fact_ratings as(
    select
        farm_fingerprint(
            concat(
                cast(user_id as string), '-',
                cast(movie_id as string), '-',
                cast(timestamp_unix as string)
            )
        )as rating_key,

        user_id,
        movie_id,
        cast(format_date('%Y%m%d', rating_date) as int64) as date_key,
        rating_year,
        rating_month,
        rating_day,
        rating_day_of_week,

        rating as rating_value,

        case
            when rating >= 4.5 then 5
            when rating >= 3.5 then 4
            when rating >= 2.5 then 3
            when rating >= 1.5 then 2
            else 1
        end as rating_star_category, 

        case
            when rating >= 4.0 then 'Positive'
            when rating >= 3.0  then 'Neutral'
            else 'Negative'   
        end as rating_sentiment,

        rating_date,
        rating_datetime,
        timestamp_unix,
        loaded_at as source_loaded_at,

        current_timestamp() as created_at,
        'silver_ratings' as source_table
    from rating_base
)

select 
    rating_key,
    user_id,
    movie_id,
    date_key,
    rating_year,
    rating_month,
    rating_day,
    rating_day_of_week,
    rating_value,
    rating_star_category,
    rating_sentiment,
    rating_date,
    rating_datetime,
    timestamp_unix,
    source_loaded_at,
    created_at,
    source_table

from fact_ratings