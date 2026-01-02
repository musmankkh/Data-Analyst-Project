{{
    config(
        materialized='table'
    )
}}

with source as (
    select 
        movieId,
        imdbId,
        tmdbId
    from {{source('bronze', 'bronze_links')}}    
),

-- Add duplicate detection
deduplicated as (
    select
        *,
        row_number() over (
            partition by movieId 
            order by 
                case when imdbId is not null then 1 else 0 end desc,
                case when tmdbId is not null then 1 else 0 end desc
        ) as duplicate_rank
    from source
),

cleaned as (
    select 
        cast(movieId as INT64) as movie_id,

        -- Clean and format IMDB ID
        case
            when trim(coalesce(cast(imdbId as string), '')) = '' then null
            when trim(coalesce(cast(imdbId as string), '')) = 'NULL' then null
            when trim(coalesce(cast(imdbId as string), '')) = 'null' then null
            else cast(imdbId as string)
        end as imdb_id_raw,
        
        -- Format IMDB ID with 'tt' prefix and padding
        case
            when trim(coalesce(cast(imdbId as string), '')) = '' then null
            when trim(coalesce(cast(imdbId as string), '')) = 'NULL' then null
            when trim(coalesce(cast(imdbId as string), '')) = 'null' then null
            else concat('tt', lpad(cast(imdbId as string), 7, '0'))
        end as imdb_id,

        -- Clean TMDB ID
        case
            when trim(coalesce(cast(tmdbId as string), '')) = '' then null
            when cast(tmdbId as string) = 'NULL' then null
            when cast(tmdbId as string) = 'null' then null
            else cast(tmdbId as INT64)
        end as tmdb_id,

        -- Data quality flags
        case when imdbId is null or trim(coalesce(cast(imdbId as string), '')) in ('', 'NULL', 'null') 
             then true else false end as is_imdb_missing,
        case when tmdbId is null or trim(coalesce(cast(tmdbId as string), '')) in ('', 'NULL', 'null') 
             then true else false end as is_tmdb_missing,
        
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
        imdb_id_raw,
        imdb_id,
        tmdb_id,
        is_imdb_missing,
        is_tmdb_missing,
        is_duplicate,
        duplicate_rank,
        loaded_at,
        dbt_run_id,
        dbt_run_started_at
    from cleaned
    where movie_id is not null
      and duplicate_rank = 1  -- Keep only first occurrence
)

select * from final