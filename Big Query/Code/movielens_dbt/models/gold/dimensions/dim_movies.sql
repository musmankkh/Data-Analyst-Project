
{{ config(
    materialized = 'table'
) }}

with movies_base as (
    select
        movie_id,
        title,
        title_clean,
        release_year,
        genres,
        genres_array,
        genre_count,
        is_genre_missing,
        has_invalid_year,
        has_invalid_genre,
        loaded_at as silver_loaded_at
    from {{ ref('silver_movies') }}
),

links_data as (
    select
        movie_id,
        imdb_id,
        imdb_id_raw,
        tmdb_id,
        is_imdb_missing,
        is_tmdb_missing
    from {{ ref('silver_links') }}
),

movie_enriched as (
    select
        m.movie_id,
        m.title,
        m.title_clean,
        m.release_year,
        
        -- Decade and era classification
        floor(m.release_year / 10) * 10 as release_decade,
        case
            when m.release_year >= 2020 then '2020s'
            when m.release_year >= 2010 then '2010s'
            when m.release_year >= 2000 then '2000s'
            when m.release_year >= 1990 then '1990s'
            when m.release_year >= 1980 then '1980s'
            when m.release_year >= 1970 then '1970s'
            when m.release_year >= 1960 then '1960s'
            else 'Before 1960s'
        end as release_decade_label,
        
        case
            when m.release_year >= 2020 then 'Modern Era'
            when m.release_year >= 2000 then 'Digital Era'
            when m.release_year >= 1980 then 'Contemporary Era'
            when m.release_year >= 1960 then 'Golden Age'
            else 'Classic Era'
        end as movie_era,
        
        -- Genre information
        m.genres,
        m.genres_array,
        m.genre_count,
        
        -- Primary genre (first in list)
        case
            when array_length(m.genres_array) > 0 then m.genres_array[offset(0)]
            else 'Unknown'
        end as primary_genre,
        
        -- Genre categories
        case
            when m.genre_count = 1 then 'Single Genre'
            when m.genre_count = 2 then 'Dual Genre'
            when m.genre_count = 3 then 'Triple Genre'
            when m.genre_count >= 4 then 'Multi Genre'
            else 'No Genre'
        end as genre_category,
        
        -- External IDs
        l.imdb_id,
        l.tmdb_id,
        
        -- Data quality flags
        m.is_genre_missing,
        m.has_invalid_year,
        m.has_invalid_genre,
        l.is_imdb_missing,
        l.is_tmdb_missing,
        
        case
            when m.is_genre_missing or m.has_invalid_year or l.is_imdb_missing 
            then 'Incomplete'
            else 'Complete'
        end as data_completeness,
        
        -- Movie age (current)
        extract(year from current_date()) - m.release_year as movie_age_years,
        
        case
            when extract(year from current_date()) - m.release_year <= 2 then 'Brand New'
            when extract(year from current_date()) - m.release_year <= 5 then 'Recent'
            when extract(year from current_date()) - m.release_year <= 10 then 'Modern'
            when extract(year from current_date()) - m.release_year <= 20 then 'Contemporary'
            when extract(year from current_date()) - m.release_year <= 40 then 'Classic'
            else 'Vintage'
        end as movie_age_category,
        
        -- SCD Type 2 fields
        m.silver_loaded_at as effective_date,
        cast('9999-12-31' as timestamp) as expiration_date,
        true as is_current,
        current_timestamp() as created_at
        
    from movies_base m
    left join links_data l using (movie_id)
)

select
    -- Primary Key
    movie_id,
    
    -- Movie Attributes
    title,
    title_clean,
    release_year,
    release_decade,
    release_decade_label,
    movie_era,
    movie_age_years,
    movie_age_category,
    
    -- Genre Attributes
    genres,
    genres_array,
    genre_count,
    primary_genre,
    genre_category,
    
    -- External Links
    imdb_id,
    tmdb_id,
    
    -- Data Quality
    is_genre_missing,
    has_invalid_year,
    has_invalid_genre,
    is_imdb_missing,
    is_tmdb_missing,
    data_completeness,
    
    -- SCD Type 2 Fields
    effective_date,
    expiration_date,
    is_current,
    created_at
    
from movie_enriched