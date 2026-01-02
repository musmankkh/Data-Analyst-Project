{{
    config(
        materialized = 'table'
    )
}}

with genre_list as (
    select distinct genre
    from {{ ref('silver_movies') }}
    cross join unnest(genres_array) as genre
),

genre_attributes as (
    select
        genre,
        farm_fingerprint(genre) as genre_key,
        case
            when genre in ('Action', 'Adventure', 'Thriller', 'War' , 'Crime') then 'Action & Adventure'
            when genre in ('Comedy', 'Romance', 'Musical') then 'light Entertainment'
            when genre in ('Drama', 'Film-Noir', 'Mystery') then 'Dramatic & Mysterious'
            when genre in ('Horror', 'Sci-Fi', 'Fantasy') then 'Speculative Fiction'
            when genre in ('Animation', 'Children') then 'Family & Animation'
            when genre in ('Documentary', 'Western') then 'Documentary & Western'
            else 'Other'
        end as genre_super_category,

         case
            when genre in ('Action', 'Thriller', 'Horror', 'War', 'Crime') then 'High Intensity'
            when genre in ('Drama', 'Mystery', 'Film-Noir', 'Documentary') then 'Thoughtful'
            when genre in ('Comedy', 'Romance', 'Musical', 'Animation') then 'Feel Good'
            when genre in ('Sci-Fi', 'Fantasy', 'Adventure') then 'Escapist'
            else 'Mixed'
        end as genre_mood,

        case
            when genre in ('Children', 'Animation', 'Musical') then 'Family'
            when genre in ('Action', 'Sci-Fi', 'Fantasy', 'Adventure') then 'Youth & Young Adult'
            when genre in ('Drama', 'Film-Noir', 'Documentary', 'Mystery') then 'Mature'
            when genre in ('Horror', 'Thriller') then 'Thrill Seekers'
            else 'General'
        end as target_audience,


        case
            when genre in ('Children', 'Animation') then 'G / PG'
            when genre in ('Comedy', 'Romance', 'Musical', 'Adventure') then 'PG / PG-13'
            when genre in ('Action', 'Thriller', 'Sci-Fi', 'Fantasy') then 'PG-13 / R'
            when genre in ('Horror', 'Crime', 'Film-Noir') then 'R'
            else 'Varies'
        end as typical_content_rating,

        case
            when genre in ('Sci-Fi', 'Fantasy') then 'Speculative'
            when genre in ('Western', 'War') then 'Historical'
            when genre in ('Documentary') then 'Real World'
            else 'Contemporary'
        end as typical_setting,

        current_timestamp()  as created_At,
        current_timestamp()  as updated_At
    from genre_list        
)


select

    genre_key,
    genre as genre_name,
    genre_super_category,
    genre_mood,
    target_audience,
    typical_content_rating,
    typical_setting,
    created_At,
    updated_At

from genre_attributes
where genre is not null
order by genre_name
    