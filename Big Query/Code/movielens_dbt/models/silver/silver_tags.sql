
{{
    config(
        materialized='table'
    )
}}

with source as (
    select * from {{ source('bronze', 'bronze_tags') }}
),

-- Add duplicate detection
deduplicated as (
    select
        *,
        row_number() over (
            partition by userId, movieId, tag, timestamp 
            order by timestamp desc
        ) as duplicate_rank
    from source
),

cleaned as (
    select
        -- Foreign keys (INT64 for BigQuery consistency)
        cast(userId as INT64) as user_id,
        cast(movieId as INT64) as movie_id,
        
        -- Clean tag: trim whitespace, lowercase for consistency
        case
            when trim(coalesce(tag, '')) = '' then null
            else lower(trim(tag))
        end as tag,
        
        -- Original tag (preserve original case)
        case
            when trim(coalesce(tag, '')) = '' then null
            else trim(tag)
        end as tag_original,
        
        -- Convert timestamp to proper datetime
        timestamp_seconds(cast(timestamp as INT64)) as tagged_at,
        
        -- Extract date components for analysis
        date(timestamp_seconds(cast(timestamp as INT64))) as tagged_date,
        extract(year from timestamp_seconds(cast(timestamp as INT64))) as tagged_year,
        extract(month from timestamp_seconds(cast(timestamp as INT64))) as tagged_month,
        extract(dayofweek from timestamp_seconds(cast(timestamp as INT64))) as tagged_day_of_week,
        
        -- Tag metrics
        length(trim(coalesce(tag, ''))) as tag_length,
        array_length(split(trim(coalesce(tag, '')), ' ')) as tag_word_count,
        
        -- Tag type classification
        case
            when regexp_contains(lower(trim(coalesce(tag, ''))), r'^\d{4}s?$') then 'decade'
            when regexp_contains(lower(trim(coalesce(tag, ''))), r'^\d{4}$') then 'year'
            when lower(trim(coalesce(tag, ''))) in ('action', 'comedy', 'drama', 'thriller', 'horror', 'sci-fi', 'romance') then 'genre'
            else 'descriptive'
        end as tag_type,
        
        -- Data quality flags
        case when trim(coalesce(tag, '')) = '' then true else false end as is_tag_missing,
        case when length(trim(coalesce(tag, ''))) < 3 then true else false end as is_tag_too_short,
        case when length(trim(coalesce(tag, ''))) > 100 then true else false end as is_tag_too_long,
        case when regexp_contains(trim(coalesce(tag, '')), r'[^\w\s\-]') 
             then true else false end as has_special_chars,
        
        -- Duplicate tracking
        duplicate_rank,
        case when duplicate_rank > 1 then true else false end as is_duplicate,
        
        -- Metadata
        current_timestamp() as loaded_at,
        '{{ invocation_id }}' as dbt_run_id,
        '{{ run_started_at }}' as dbt_run_started_at
        
    from deduplicated
),

final as (
    select
        user_id,
        movie_id,
        tag,
        tag_original,
        tag_length,
        tag_word_count,
        tag_type,
        tagged_at,
        tagged_date,
        tagged_year,
        tagged_month,
        tagged_day_of_week,
        is_tag_missing,
        is_tag_too_short,
        is_tag_too_long,
        has_special_chars,
        is_duplicate,
        duplicate_rank,
        loaded_at,
        dbt_run_id,
        dbt_run_started_at
        
    from cleaned
    where user_id is not null
      and movie_id is not null
      and tag is not null  -- Remove any null tags
      and not is_tag_too_short  -- Filter out very short tags
      and duplicate_rank = 1  -- Keep only first occurrence
)

select * from final