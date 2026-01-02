
{{ config(
    materialized = 'table'
  
) }}

with all_tags as (
    -- User tags
    select distinct
        tag,
        'User Generated' as tag_source
    from {{ ref('silver_tags') }}
    where tag is not null
    
    union distinct
    
    -- Genome tags
    select distinct
        lower(trim(tag)) as tag,
        'Genome System' as tag_source
    from {{ ref('silver_genome_tags') }}
    where tag is not null
),

tag_attributes as (
    select
        tag,
        
        -- Generate surrogate key
        farm_fingerprint(tag) as tag_key,
        
        -- Source (if appears in both, mark as 'Both')
        case
            when count(distinct tag_source) > 1 then 'Both'
            else max(tag_source)
        end as tag_source,
        
        -- Tag characteristics
        length(tag) as tag_length,
        array_length(split(tag, ' ')) as word_count,
        
        -- Tag type classification
        case
            when regexp_contains(tag, r'^\d{4}s?$') then 'Decade/Year'
            when tag in ('action', 'comedy', 'drama', 'thriller', 'horror', 
                        'sci-fi', 'romance', 'documentary', 'fantasy', 'mystery',
                        'animation', 'adventure', 'crime', 'war', 'western',
                        'musical', 'film-noir', 'children') then 'Genre'
            when tag in ('dark', 'funny', 'sad', 'scary', 'intense', 'light',
                        'depressing', 'heartwarming', 'suspenseful', 'emotional') then 'Mood/Tone'
            when regexp_contains(tag, r'based on') then 'Adaptation'
            when regexp_contains(tag, r'director|actor|actress|star') then 'People'
            when regexp_contains(tag, r'twist|ending|plot') then 'Story Element'
            when regexp_contains(tag, r'visual|cinematography|effects') then 'Technical'
            else 'Descriptive'
        end as tag_category,
        
        -- Sentiment
        case
            when tag in ('great', 'excellent', 'amazing', 'brilliant', 'masterpiece',
                        'classic', 'perfect', 'best', 'favorite', 'love') then 'Positive'
            when tag in ('bad', 'terrible', 'boring', 'awful', 'worst', 'hate',
                        'overrated', 'disappointing') then 'Negative'
            else 'Neutral/Descriptive'
        end as tag_sentiment,
        
       
        -- Metadata
        current_timestamp() as created_at,
        current_timestamp() as updated_at
        
    from all_tags
    group by tag
)

select
    -- Primary Key
    tag_key,
    
    -- Tag Attributes
    tag as tag_name,
    tag_source,
    tag_length,
    word_count,
    tag_category,
    tag_sentiment,

    
    -- Metadata
    created_at,
    updated_at
    
from tag_attributes
order by tag_name