{{config(
    materialized = 'table',
    cluster_by = ['movie_id', 'tag_id'])
}}

with genome_scores_base as (
    select
        gs.movie_id,
        gs.tag_id,
        gs.relevance,
        gs.relevance_rounded,
        gs.relevance_category,
        gt.tag,
        gt.tag_original,
        gs.loaded_at

    from {{ ref('silver_genome_scores') }} as gs    
    left join {{ ref('silver_genome_tags') }} as gt
        on gs.tag_id = gt.tag_id
),

fact_genome_scores as (
    select
        farm_fingerprint(
            concat(
                cast(movie_id as string), '-',
                cast(tag_id as string)
            )
        ) as genome_score_key,

        movie_id,
        tag_id,
        farm_fingerprint(lower(trim(tag))) as tag_key,
        
        tag,
        tag_original,

        relevance as relevance_score,
        relevance_rounded,

        relevance_category,

        case
            when relevance >= 0.9 then 'Extremely Relevant'
            when relevance >= 0.8 then 'Highly Relevant'
            when relevance >= 0.7 then 'Very Relevant'
            when relevance >= 0.5 then 'Moderately Relevant'
            when relevance >= 0.3 then 'Somewhat Relevant'
            when relevance >= 0.2 then 'Slightly Relevant'
            else 'Marginally Relevant'
        end as relevance_detail_category,

        floor(relevance * 10) /  10  as relevance_band,

        case when relevance >= 0.8 then 1 else 0 end as is_highly_relevant,
        case when relevance >= 0.5 then 1 else 0 end as is_medium_relevant,
        case when relevance < 0.2 then 1 else 0 end as is_lowly_relevant,

        loaded_at as source_loaded_at,
        current_timestamp() as created_at,
        'silver_genome_scores' as source_table

    from genome_scores_base

)

select 
    genome_score_key,
    movie_id,
    tag_id,
    tag_key,
    
    tag,
    tag_original,

    relevance_score,
    relevance_rounded,

    relevance_category,
    relevance_detail_category,
    relevance_band,

    is_highly_relevant,
    is_medium_relevant,
    is_lowly_relevant,

    source_loaded_at,
    created_at,
    source_table

from fact_genome_scores    

