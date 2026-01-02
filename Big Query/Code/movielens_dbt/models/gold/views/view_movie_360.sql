{{config(
    materialized = 'view'
)}}

with movie_ratings_agg as (
    select
        movie_id,
        count(*) as total_ratings,
        round(avg(rating_value), 2) as avg_rating,
        round(stddev(rating_value), 2) as rating_stddev,
        approx_quantiles(rating_value, 100)[offset(50)] as rating_median,
        countif(rating_sentiment = 'Positive') as positive_ratings,
        countif(rating_sentiment = 'Negative') as negative_ratings,
        count(distinct user_id) as unique_raters,
        min(rating_date) as first_rating_date,
        max(rating_date) as last_rating_date
    from {{ ref('fact_ratings') }}
    group by movie_id    

),

movie_tags_agg as (
    select
        movie_id,
        count(*) as total_user_tags,
        count(distinct user_id) as unique_taggers,
        count(distinct tag) as unique_tags_count,
        array_agg(distinct tag order by tag limit 10) as top_user_tags
    from {{ ref('fact_user_tags') }}
    group by movie_id    
),

movie_genome_agg as (
    select
        movie_id,
        count(*) as total_genome_tags,
        round(avg(relevance_score), 3) as avg_genome_relevance,
        countif(is_highly_relevant = 1) as high_relevance_tags,
        array_agg(
            struct(tag, relevance_score)
            order by relevance_score desc
            limit 5
        ) as top_genome_tags
    from {{ ref('fact_genome_scores') }}
    group by movie_id

)

select
      dm.movie_id,
      dm.title,
      dm.title_clean,
      dm.release_year,
      dm.release_decade,
      dm.release_decade_label,
      dm.movie_era,
      dm.movie_age_years,
      dm.movie_age_category,
      dm.genres,
      dm.genres_array,
      dm.genre_count,
      dm.primary_genre,
      dm.genre_category,
      dm.imdb_id,
      dm.tmdb_id,
      dm.data_completeness,

      coalesce(mra.total_ratings, 0) as total_ratings,
      mra.avg_rating,
      mra.rating_stddev,
      mra.rating_median,
      coalesce(mra.positive_ratings, 0) as positive_ratings,
      coalesce(mra.negative_ratings, 0) as negative_ratings,
      coalesce(mra.unique_raters, 0) as unique_raters,
      mra.first_rating_date,
      mra.last_rating_date,

      coalesce(mta.total_user_tags, 0) as total_user_tags,
      coalesce(mta.unique_taggers, 0) as unique_taggers,
      coalesce(mta.unique_tags_count, 0) as unique_tags_count,
      mta.top_user_tags,


      coalesce(mga.total_genome_tags, 0) as total_genome_tags,
      mga.avg_genome_relevance,
      coalesce(mga.high_relevance_tags, 0) as high_relevance_tags,
      mga.top_genome_tags,

    case
        when mra.avg_rating >= 4.0 and mra.total_ratings >= 100 then 'Highly Rated Popular'
        when mra.avg_rating >= 4.0 then 'Highly Rated'
        when mra.total_ratings >= 1000 then 'Very Popular'
        when mra.total_ratings >= 100 then 'Popular'
        else 'Niche'
    end as movie_performance_tier,

     round(
        (coalesce(mra.avg_rating, 0) / 5.0) * 0.5 +
        (least(log10(coalesce(mra.total_ratings, 0) + 1) / 4, 1.0)) * 0.3 +
        (coalesce(mga.avg_genome_relevance, 0)) * 0.2,
        3
    ) as overall_movie_score,
    
    current_timestamp() as view_generated_at

from {{ ref('dim_movies') }} dm
left join movie_ratings_agg mra using (movie_id)
left join movie_tags_agg mta using (movie_id)
left join movie_genome_agg mga using (movie_id)

         


        