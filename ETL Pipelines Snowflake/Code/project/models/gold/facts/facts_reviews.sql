{{config(
    materialized='table',
    unique_key='review_key'
)}}

SELECT
    {{dbt_utils.generate_surrogate_key(['review_id'])}} AS review_key,
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    DATEDIFF(day, review_creation_date, review_answer_timestamp) AS response_time_days,
    CASE
        WHEN review_score >=4 THEN 'Positive'
        WHEN review_score >=3 THEN 'Neutral'
        ELSE 'Negative'
    END AS review_sentiment,
    CURRENT_TIMESTAMP AS created_at

FROM {{source('silver', 'olist_order_reviews_dataset')}}         