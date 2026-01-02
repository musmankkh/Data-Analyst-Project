{{
    config(
        materialized = 'table',
        partition_by = {
            "field": "tagged_date",
            "data_type": "date"
        },
        cluster_by = [
            "user_id",
            "movie_id",
            "tag"
        ]
    )
}}


with tag_base as (
    select
        user_id,
        movie_id,
        tag,
        tag_original,
        tag_type,
        tagged_at,
        tagged_date,
        tagged_year,
        tagged_month,
      
        tag_length,
        tag_word_count,
        loaded_at
    from {{ ref('silver_tags') }}
),

fact_user_tags as (
    select
        farm_fingerprint(
            concat(
                cast(user_id as string), '-',
                cast(movie_id as string), '-',
                tag, '-',
                cast(unix_seconds(tagged_at) as string)
            )
            ) as tag_event_key,

        user_id,
        movie_id,
        farm_fingerprint(tag) as tag_key,
        cast(format_date('%Y%m%d', tagged_date) as int64) as date_key,

        tag,
        tag_type,
        tag_original,
        tag_length,
        tag_word_count,

        tagged_year,
        tagged_month,

        tagged_at,
        tagged_date,

        case
            when tag_word_count = 1 then 'Single Word'
            when tag_word_count = 2 then 'Two Words'
            else 'Multi Word'
        end as tag_complexity,

        loaded_at as source_loaded_at,
        current_timestamp() as created_at,
        'silver_tags' as source_table

    from tag_base
)

select
    tag_event_key,
    user_id,
    movie_id,
    tag_key,
    date_key,
    tag,
    tag_type,
    tag_original,
    tag_length,
    tag_word_count,
    tagged_year,
    tagged_month,
    tagged_at,
    tagged_date,
    tag_complexity,
    source_loaded_at,
    created_at,
    source_table

from fact_user_tags




