{{config(
    materialized = 'view'
)}}

with user_tag_stats as (
    select 
        tag_key,
        tag,
        tag_type,
        count(*) as user_tag_uses,
        count(distinct user_id) as unique_users,
        count(distinct movie_id) as unique_movies_tagged,
        min(tagged_date) as first_used,
        max(tagged_date) as last_used,
        countif(tagged_date >= date_sub(current_date(), interval 365 day)) as uses_last_year,
        countif(tagged_date >= date_sub(current_date(), interval 90 day)) as uses_last_90d
    from {{ ref('fact_user_tags') }}
    group by tag_key, tag, tag_type    


),

genome_tag_stats as (
    select
        tag_key,
        tag,
        count(*) as genome_tag_uses,
        round(avg(relevance_score), 3) as avg_relevance,
        round(stddev(relevance_score), 3) as relevance_stddev,
        count(distinct movie_id) as genome_movie_count,
        countif(is_highly_relevant = 1) as high_relevance_count
    from {{ ref('fact_genome_scores') }}
    group by tag_key, tag
),        

tag_rating_correlation as (
    select
        fut.tag_key,
        fut.tag,
        round(avg(fr.rating_value), 2) as avg_rating_tagged_movies,
        count(distinct fr.rating_key) as ratings_for_tagged_movies
    from {{ ref('fact_user_tags') }} fut
    left join {{ ref('fact_ratings') }} fr
        on fut.movie_id = fr.movie_id
    group by fut.tag_key, fut.tag
),

comprehensive_tag_analysis as (
    select
        coalesce(uts.tag_key, gts.tag_key) as tag_key,
        coalesce(uts.tag, gts.tag) as tag,
  
        dt.tag_source,
        dt.tag_category,
        dt.tag_sentiment,
       
        dt.tag_length,
        dt.word_count,
        coalesce(uts.tag_type, 'descriptive') as tag_type,
        
   
        coalesce(uts.user_tag_uses, 0) as user_tag_uses,
        coalesce(uts.unique_users, 0) as unique_users,
        coalesce(uts.unique_movies_tagged, 0) as unique_movies_tagged,
        uts.first_used,
        uts.last_used,
        coalesce(uts.uses_last_year, 0) as uses_last_year,
        coalesce(uts.uses_last_90d, 0) as uses_last_90d,
        
        
        coalesce(gts.genome_tag_uses, 0) as genome_tag_uses,
        gts.avg_relevance as genome_avg_relevance,
        gts.relevance_stddev as genome_relevance_stddev,
        coalesce(gts.genome_movie_count, 0) as genome_movie_count,
        coalesce(gts.high_relevance_count, 0) as high_relevance_count,
        
      
        trc.avg_rating_tagged_movies,
        coalesce(trc.ratings_for_tagged_movies, 0) as ratings_for_tagged_movies,
        
   
        coalesce(uts.unique_movies_tagged, 0) + coalesce(gts.genome_movie_count, 0) as total_movie_coverage
        
    from user_tag_stats uts
    full outer join genome_tag_stats gts using (tag_key, tag)
    left join {{ ref('dim_tags') }} dt
        on coalesce(uts.tag_key, gts.tag_key) = dt.tag_key
    left join tag_rating_correlation trc
        on coalesce(uts.tag_key, gts.tag_key) = trc.tag_key
),

tag_rankings as (
    select
        *,
        row_number() over (order by user_tag_uses desc) as rank_by_usage,
        row_number() over (order by unique_movies_tagged desc) as rank_by_coverage,
        row_number() over (order by genome_avg_relevance desc) as rank_by_relevance,
        row_number() over (order by avg_rating_tagged_movies desc) as rank_by_quality,

        case
            when user_tag_uses >= 1000 then 'Very Popular'
            when user_tag_uses >= 100  then 'Popular'
            when user_tag_uses >= 20  then 'Moderately Used'
            else 'Niche'
        end as popularity_tier,

        case
            when genome_avg_relevance >= 0.8 then 'Highly Relevant'
            when genome_avg_relevance >= 0.5 then 'Moderately Relevant'
            when genome_avg_relevance >= 0.2 then 'Somewhat Relevant'
            when genome_avg_relevance is not null then 'Low Relevance'
            else 'Not in Genome'
        end as relevance_category,
        
        case
            when uses_last_90d > 0 then 'Recently Active'
            when uses_last_year > 0 then 'Active'
            when last_used is not null then 'Historical'
            else 'Genome Only'
        end as activity_status,
        
  
        round(
            (least(log10(user_tag_uses + 1) / 3, 1.0)) * 0.3 +
            (coalesce(genome_avg_relevance, 0)) * 0.4 +
            (least(unique_movies_tagged / 100.0, 1.0)) * 0.3,
            3
        ) as tag_quality_score
        
    from comprehensive_tag_analysis
)    

select 
    tag_key,
    tag as tag_name,
    tag_source,
    tag_category,
    tag_sentiment,
    tag_length,
    word_count,
    tag_type,
    user_tag_uses,
    unique_users,
    unique_movies_tagged,
    first_used,
    last_used,
    uses_last_year,
    uses_last_90d,
    genome_tag_uses,
    genome_avg_relevance,
    genome_relevance_stddev,
    genome_movie_count,
    high_relevance_count,
    avg_rating_tagged_movies,
    ratings_for_tagged_movies,
    total_movie_coverage,
    rank_by_usage,
    rank_by_coverage,
    rank_by_relevance,
    rank_by_quality,
    popularity_tier,
    relevance_category,
    activity_status,
    tag_quality_score,
    current_timestamp() as view_generated_at

from tag_rankings
where tag is not null
order by user_tag_uses desc