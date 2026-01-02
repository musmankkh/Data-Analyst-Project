{{ config(
    materialized = 'table'
) }}

with source as (
    select
        movieId,
        title,
        genres
    from {{ source('bronze', 'bronze_movies') }}
),

-- Add duplicate detection
deduplicated as (
    select
        *,
        row_number() over (
            partition by movieId 
            order by 
                length(trim(coalesce(title, ''))) desc,
                length(trim(coalesce(genres, ''))) desc
        ) as duplicate_rank
    from source
),

-- Identify ALL year patterns
initial_clean as (
    select
        SAFE_CAST(movieId AS INT64) as movie_id,
        trim(coalesce(title, '')) as title_raw,
        trim(coalesce(genres, '')) as genres_raw,
        
        -- Pattern detection flags for different year formats
        -- Standard single year: (1995)
        regexp_contains(trim(coalesce(title, '')), r'\(\d{4}\)$') as has_standard_year,
        
        -- Double closing parenthesis: (1983))
        regexp_contains(trim(coalesce(title, '')), r'\(\d{4}\)\)$') as has_double_paren,
        
        -- TV series with hyphen: (2007-) or (1975-1979)
        regexp_contains(trim(coalesce(title, '')), r'\(\d{4}-\d{0,4}\)$') as has_hyphen_range,
        
        -- TV series with en dash: (2009– ) or (2009–2015)
        regexp_contains(trim(coalesce(title, '')), r'\(\d{4}[–—]\s?\d{0,4}\)$') as has_endash_range,
        
        -- Genre check
        trim(coalesce(genres, '')) in ('', '(no genres listed)') as is_genre_empty,
        
        duplicate_rank
    from deduplicated
),

-- Extract year and clean title based on detected pattern
year_extraction as (
    select
        movie_id,
        title_raw,
        genres_raw,
        is_genre_empty,
        duplicate_rank,
        
        -- Determine which pattern matched (priority order matters)
        case
            when has_double_paren then 'double_paren'
            when has_endash_range then 'endash_range'
            when has_hyphen_range then 'hyphen_range'
            when has_standard_year then 'standard_year'
            else 'no_year'
        end as year_pattern_type,
        
        -- Extract start year based on pattern
        case
            -- Double parenthesis: extract from (YYYY))
            when has_double_paren then
                SAFE_CAST(regexp_extract(title_raw, r'\((\d{4})\)\)$') AS INT64)
            
            -- En dash range: extract from (YYYY–) or (YYYY–YYYY)
            when has_endash_range then
                SAFE_CAST(regexp_extract(title_raw, r'\((\d{4})[–—]') AS INT64)
            
            -- Hyphen range: extract from (YYYY-) or (YYYY-YYYY)
            when has_hyphen_range then
                SAFE_CAST(regexp_extract(title_raw, r'\((\d{4})-') AS INT64)
            
            -- Standard year: extract from (YYYY)
            when has_standard_year then
                SAFE_CAST(regexp_extract(title_raw, r'\((\d{4})\)$') AS INT64)
            
            else null
        end as year_start,
        
        -- Extract end year (only for ranges)
        case
            -- En dash range with end year: (YYYY–YYYY)
            when has_endash_range and regexp_contains(title_raw, r'\(\d{4}[–—]\s?\d{4}\)$') then
                SAFE_CAST(regexp_extract(title_raw, r'\(\d{4}[–—]\s?(\d{4})\)$') AS INT64)
            
            -- Hyphen range with end year: (YYYY-YYYY)
            when has_hyphen_range and regexp_contains(title_raw, r'\(\d{4}-\d{4}\)$') then
                SAFE_CAST(regexp_extract(title_raw, r'\(\d{4}-(\d{4})\)$') AS INT64)
            
            else null
        end as year_end,
        
        -- Clean title by removing year pattern
        case
            when has_double_paren then
                trim(regexp_replace(title_raw, r'\s*\(\d{4}\)\)$', ''))
            when has_endash_range then
                trim(regexp_replace(title_raw, r'\s*\(\d{4}[–—]\s?\d{0,4}\)$', ''))
            when has_hyphen_range then
                trim(regexp_replace(title_raw, r'\s*\(\d{4}-\d{0,4}\)$', ''))
            when has_standard_year then
                trim(regexp_replace(title_raw, r'\s*\(\d{4}\)$', ''))
            when title_raw = '' then null
            else title_raw
        end as title_clean
        
    from initial_clean
),

-- Apply transformations
transformed as (
    select
        movie_id,
        
        -- Title fields
        case when title_raw = '' then null else title_raw end as title,
        title_clean,
        
        -- Single release_year column (uses start year for ranges)
        case
            when year_start between 1800 and extract(year from current_date()) + 5
                then year_start
            else null
        end as release_year,
        
        -- Content type inference
        case
            when year_pattern_type in ('hyphen_range', 'endash_range') then 'TV Series'
            when year_pattern_type in ('standard_year', 'double_paren') then 'Movie'
            else 'Unknown'
        end as content_type,
        
        -- Genre fields
        case when is_genre_empty then null else genres_raw end as genres,
        case when is_genre_empty then [] else split(genres_raw, '|') end as genres_array,
        
        -- Data quality flags
        title_raw = '' as is_title_missing,
        is_genre_empty as is_genre_missing,
        year_pattern_type,
        
        -- Additional quality flags
        case
            when year_pattern_type = 'double_paren' then true
            else false
        end as has_malformed_year,
        
        case
            when year_start is not null 
                and year_start not between 1800 and extract(year from current_date()) + 5
                then true
            else false
        end as has_invalid_year,
        
        duplicate_rank,
        case when duplicate_rank > 1 then true else false end as is_duplicate
        
    from year_extraction
),

-- Final validation and computed fields
final as (
    select
        movie_id,
        title,
        title_clean,
        release_year,
        content_type,
        
        genres,
        genres_array,
        array_length(genres_array) as genre_count,
        
        -- Data quality flags
        is_title_missing,
        is_genre_missing,
        has_malformed_year,
        has_invalid_year,
        year_pattern_type,
        
        -- Genre validation (check against known MovieLens genres)
        case 
            when array_length(genres_array) > 0 and exists (
                select 1 from unnest(genres_array) as g 
                where g not in ('Action','Adventure','Animation','Children','Comedy',
                                'Crime','Documentary','Drama','Fantasy','Film-Noir',
                                'Horror','Musical','Mystery','Romance','Sci-Fi',
                                'Thriller','War','Western','IMAX')
            ) then true 
            else false 
        end as has_invalid_genre,
        
        is_duplicate,
        duplicate_rank,
        
        -- Metadata
        current_timestamp() as loaded_at,
        '{{ invocation_id }}' as dbt_run_id,
        '{{ run_started_at }}' as dbt_run_started_at
        
    from transformed
    where movie_id is not null
      and duplicate_rank = 1  -- Keep only first occurrence
)

select * from final