{{
    config(
        materialized='table'
    )
}}

with source as (
    select * from {{ source('bronze', 'bronze_genome_scores') }}
),

-- Add duplicate detection
deduplicated as (
    select
        *,
        row_number() over (
            partition by movieId, tagId 
            order by relevance desc
        ) as duplicate_rank
    from source
),

cleaned as (
    select
        -- Foreign keys (INT64 for BigQuery consistency)
        cast(movieId as INT64) as movie_id,
        cast(tagId as INT64) as tag_id,
        
        -- Relevance score (must be between 0 and 1)
        cast(relevance as float64) as relevance,
        
        -- Categorize relevance levels for analysis
        case
            when cast(relevance as float64) >= 0.8 then 'high'
            when cast(relevance as float64) >= 0.5 then 'medium'
            when cast(relevance as float64) >= 0.2 then 'low'
            else 'very_low'
        end as relevance_category,
        
        -- Round relevance for easier grouping
        round(cast(relevance as float64), 2) as relevance_rounded,
        
        -- Data quality flags
        case when cast(relevance as float64) < 0.0 or cast(relevance as float64) > 1.0 
             then true else false end as is_relevance_out_of_range,
        
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
        movie_id,
        tag_id,
        relevance,
        relevance_rounded,
        relevance_category,
        is_relevance_out_of_range,
        is_duplicate,
        duplicate_rank,
        loaded_at,
        dbt_run_id,
        dbt_run_started_at
        
    from cleaned
    where movie_id is not null
      and tag_id is not null
      and relevance is not null
      and relevance >= 0.0 and relevance <= 1.0  -- Validate relevance range
      and duplicate_rank = 1 
)

select * from final