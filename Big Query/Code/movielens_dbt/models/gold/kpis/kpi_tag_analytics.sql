
{{ config(
    materialized = 'table'
) }}

select
 
    tag_name,
    tag_key,
    
   
  
    tag_category,
    tag_sentiment,
    tag_type,
    

    user_tag_uses,
    unique_users as users_who_tagged,
    unique_movies_tagged,
   
    
 
    genome_tag_uses,
    genome_avg_relevance,
    genome_movie_count,
    high_relevance_count,
    
  
    avg_rating_tagged_movies,
    ratings_for_tagged_movies,
    
   
    total_movie_coverage,
    
    
    rank_by_usage as rank_popularity,
    rank_by_coverage as rank_movie_coverage,
    rank_by_relevance as rank_genome_relevance,
    rank_by_quality as rank_rating_correlation,
    

  
    relevance_category,
    activity_status,
    
 
    tag_quality_score,
    
   
    round(user_tag_uses * 1.0 / nullif(unique_users, 0), 1) as avg_uses_per_user,
    round(user_tag_uses * 1.0 / nullif(unique_movies_tagged, 0), 1) as avg_tags_per_movie,
    
    round(uses_last_year * 100.0 / nullif(user_tag_uses, 0), 1) as pct_uses_last_year,
    round(uses_last_90d * 100.0 / nullif(user_tag_uses, 0), 1) as pct_uses_last_90d,
    
    case
        when user_tag_uses >= 100 and genome_avg_relevance >= 0.7 then 'Popular & Relevant'
        when user_tag_uses >= 100 and genome_avg_relevance < 0.7 then 'Popular but Less Relevant'
        when user_tag_uses < 100 and genome_avg_relevance >= 0.7 then 'Niche but Relevant'
        when genome_avg_relevance is null and user_tag_uses >= 100 then 'Popular (No Genome Data)'
        when genome_avg_relevance is null then 'Niche (No Genome Data)'
        else 'Niche & Less Relevant'
    end as tag_performance_segment,
    
 
    case
        when uses_last_90d > 0 and user_tag_uses < 50 then 'Emerging'
        when uses_last_90d > 0 and user_tag_uses >= 50 then 'Growing'
        when uses_last_year > 0 and uses_last_90d = 0 then 'Mature'
        when last_used is not null then 'Declining'
        else 'Genome Only'
    end as tag_lifecycle_stage,
    
    
    first_used,
    last_used,
    date_diff(current_date(), last_used, day) as days_since_last_use,
    
   
    current_timestamp() as kpi_generated_at
    
from {{ ref('view_tag_intelligence') }}
where user_tag_uses > 0 or genome_tag_uses > 0
order by user_tag_uses desc