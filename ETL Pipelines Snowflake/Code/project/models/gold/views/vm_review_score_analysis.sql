{{config(
    materializated='view'
)}}

SELECT 
    fr.order_id,
    fr.review_score,
    fr.review_creation_date,
    fr.review_sentiment,
    fr.response_time_days AS time_to_review,

    SUM(CASE WHEN fr.review_score <=2 THEN 1 ELSE 0 END) OVER() AS n_negative_reviews,
    SUM(CASE WHEN fr.review_score >=4 THEN 1 ELSE 0 END) OVER() AS n_positive_reviews

FROM {{ref('facts_reviews')}} fr   