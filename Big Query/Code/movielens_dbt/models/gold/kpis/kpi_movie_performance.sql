{{config(
    materialized = 'table',
    partition_by = {
        "field": "release_year",
        "data_type": "int64",
        "range": {
            "start": 1900,
            "end": 2030,
            "interval": 10
}}
)}}


select 
    movie_id,
    title,
    title_clean,
    release_year,
    release_decade_label as release_decade,
    movie_era,
    
    primary_genre,
    genre_category,
    genre_count,

    imdb_id,
    tmdb_id,

    total_ratings,
    avg_rating,
    rating_median,
    rating_stddev,
    positive_ratings,
    negative_ratings,
    unique_raters,

    round(positive_ratings * 100.0 / nullif(total_ratings, 0), 1) as pct_positive_ratings,
    round(negative_ratings * 100.0 / nullif(total_ratings, 0), 1) as pct_negative_ratings,
    
    total_user_tags,
    unique_taggers,
    total_genome_tags,
    high_relevance_tags,

    movie_performance_tier,

    case
        when total_ratings >= 10000 then 'Blockbuster (10K+)'
        when total_ratings >= 5000 then 'Major Hit (5K+)'
        when total_ratings >= 1000 then 'Very Popular (1K+)'
        when total_ratings >= 500 then 'Popular (500+)'
        when total_ratings >= 100 then 'Moderate (100+)'
        when total_ratings >= 50 then 'Niche (50+)'
        else 'Limited (<50)'
    end as popularity_tier,

    case
        when rating_stddev >= 1.5 then 'Highly Controversial'
        when rating_stddev >= 1.2 then 'Somewhat Controversial'
        when rating_stddev >= 1.0 then 'Mixed Reviews'
        else 'Consensus'
    end as controversy_level,


     case
        when avg_rating >= 4.5 then '5 Star (4.5+)'
        when avg_rating >= 4.0 then '4+ Star (4.0-4.5)'
        when avg_rating >= 3.5 then '3.5+ Star'
        when avg_rating >= 3.0 then '3+ Star'
        when avg_rating >= 2.5 then '2.5+ Star'
        else 'Below 2.5 Star'
    end as rating_tier,


    overall_movie_score as composite_score,

    first_rating_date,
    last_rating_date,
    date_diff(last_rating_date, first_rating_date, day) as rating_span_days,

    data_completeness,

    current_timestamp() as kpi_generated_at

from {{ref('view_movie_360')}}

where total_ratings > 0



