{{ config(
    materialized = 'table'
) }}

with source_data as (
    select 
        userId,
        movieId,
        rating,
        timestamp
    from {{ source('bronze', 'bronze_ratings') }}
),

-- Step 1: Data quality validation flags
quality_checks as (
    select
        userId,
        movieId,
        rating,
        timestamp,
        
        -- NULL checks
        case when userId is null then 1 else 0 end as is_null_user_id,
        case when movieId is null then 1 else 0 end as is_null_movie_id,
        case when rating is null then 1 else 0 end as is_null_rating,
        case when timestamp is null then 1 else 0 end as is_null_timestamp,
        
        -- Empty/zero checks
        case when cast(userId as string) = '' or userId = 0 then 1 else 0 end as is_empty_user_id,
        case when cast(movieId as string) = '' or movieId = 0 then 1 else 0 end as is_empty_movie_id,
        
        -- Rating validations (MovieLens uses 0.5 to 5.0 in 0.5 increments)
        case when rating < 0.5 or rating > 5.0 then 1 else 0 end as is_invalid_rating_range,
        case when mod(cast(rating * 2 as int64), 1) != 0 then 1 else 0 end as is_invalid_rating_increment,
        
        -- Timestamp validations
        case when timestamp <= 0 then 1 else 0 end as is_invalid_timestamp,
        case when timestamp > unix_seconds(current_timestamp()) then 1 else 0 end as is_future_timestamp,
        
        -- Duplicate detection (same user rating same movie at same time)
        row_number() over (
            partition by userId, movieId, timestamp 
            order by rating desc
        ) as duplicate_rank
        
    from source_data
),

-- Step 2: Apply cleaning rules and transformations
cleaned_data as (
    select
        -- Original fields with type casting
        cast(userId as int64) as user_id,
        cast(movieId as int64) as movie_id,
        round(cast(rating as numeric), 1) as rating,
        cast(timestamp as int64) as timestamp_unix,
        
        -- Timestamp conversions
        timestamp_seconds(cast(timestamp as int64)) as rating_datetime,
        date(timestamp_seconds(cast(timestamp as int64))) as rating_date,
        extract(year from timestamp_seconds(cast(timestamp as int64))) as rating_year,
        extract(month from timestamp_seconds(cast(timestamp as int64))) as rating_month,
        extract(day from timestamp_seconds(cast(timestamp as int64))) as rating_day,
        format_timestamp('%A', timestamp_seconds(cast(timestamp as int64))) as rating_day_of_week,
        
        -- Data quality summary flag
        case 
            when is_null_user_id = 1 or is_null_movie_id = 1 or is_null_rating = 1 or is_null_timestamp = 1
                then 'CRITICAL: Missing Required Field'
            when is_empty_user_id = 1 or is_empty_movie_id = 1
                then 'CRITICAL: Empty/Zero ID'
            when is_invalid_rating_range = 1
                then 'ERROR: Rating Out of Range'
            when is_invalid_rating_increment = 1
                then 'ERROR: Invalid Rating Increment'
            when is_invalid_timestamp = 1 or is_future_timestamp = 1
                then 'ERROR: Invalid Timestamp'
            when duplicate_rank > 1
                then 'WARNING: Duplicate Record'
            else 'VALID'
        end as data_quality_status,
        
        -- Individual quality flags for analysis (kept for auditing)
        duplicate_rank,
        
        -- Metadata
        current_timestamp() as loaded_at,
        '{{ invocation_id }}' as dbt_run_id,
        '{{ run_started_at }}' as dbt_run_started_at
        
    from quality_checks
    where 
        -- Filter out records with critical data quality issues
        is_null_user_id = 0
        and is_null_movie_id = 0
        and is_null_rating = 0
        and is_null_timestamp = 0
        and is_empty_user_id = 0
        and is_empty_movie_id = 0
        and is_invalid_rating_range = 0
        and is_invalid_rating_increment = 0
        and is_invalid_timestamp = 0
        and is_future_timestamp = 0
        -- Keep only first occurrence of duplicates
        and duplicate_rank = 1
)

-- Step 3: Final output with clean, validated data
select
    user_id,
    movie_id,
    rating,
    timestamp_unix,
    rating_datetime,
    rating_date,
    rating_year,
    rating_month,
    rating_day,
    rating_day_of_week,
    data_quality_status,
    duplicate_rank,
    loaded_at,
    dbt_run_id,
    dbt_run_started_at
from cleaned_data
order by user_id, rating_datetime