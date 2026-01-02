{{
    config(
        materialized='table'
    )
}}

with source as (
    select * from {{ source('bronze', 'bronze_genome_tags') }}
),

-- Add duplicate detection
deduplicated as (
    select
        *,
        row_number() over (
            partition by tagId 
            order by length(trim(coalesce(tag, ''))) desc
        ) as duplicate_rank
    from source
),

cleaned as (
    select
        -- Primary key (INT64 for BigQuery consistency)
        cast(tagId as INT64) as tag_id,
        
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
        
        -- Tag length
        length(trim(coalesce(tag, ''))) as tag_length,
        
        -- Data quality flags
        case when trim(coalesce(tag, '')) = '' then true else false end as is_tag_missing,
        case when length(trim(coalesce(tag, ''))) < 2 then true else false end as is_tag_too_short,
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
        tag_id,
        tag,
        tag_original,
        tag_length,
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
    where tag_id is not null
      and tag is not null  -- Ensure no null tags
      and not is_tag_too_short  -- Filter out very short tags
      and duplicate_rank = 1  -- Keep only first occurrence
)

select * from final