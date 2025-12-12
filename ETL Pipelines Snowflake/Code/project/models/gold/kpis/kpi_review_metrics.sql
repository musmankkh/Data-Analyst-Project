
{{
    config(
        materialized='view'
    )
}}

WITH review_data AS (
    SELECT * FROM {{ ref('vm_review_score_analysis') }}
)

SELECT
    'Review & Experience Metrics' AS kpi_category,
    
    -- Total Reviews
    COUNT(DISTINCT order_id) AS total_reviews,
    
    -- Average Review Score
    ROUND(AVG(review_score), 2) AS avg_review_score,
    
    -- % 5-Star Reviews
    ROUND(
        COUNT(DISTINCT CASE WHEN review_score = 5 THEN order_id END) * 100.0 
        / NULLIF(COUNT(DISTINCT order_id), 0),
        2
    ) AS five_star_percentage,
    
    -- % 4-Star Reviews
    ROUND(
        COUNT(DISTINCT CASE WHEN review_score = 4 THEN order_id END) * 100.0 
        / NULLIF(COUNT(DISTINCT order_id), 0),
        2
    ) AS four_star_percentage,
    
    -- % 3-Star Reviews
    ROUND(
        COUNT(DISTINCT CASE WHEN review_score = 3 THEN order_id END) * 100.0 
        / NULLIF(COUNT(DISTINCT order_id), 0),
        2
    ) AS three_star_percentage,
    
    -- % 2-Star Reviews
    ROUND(
        COUNT(DISTINCT CASE WHEN review_score = 2 THEN order_id END) * 100.0 
        / NULLIF(COUNT(DISTINCT order_id), 0),
        2
    ) AS two_star_percentage,
    
    -- % 1-Star Reviews
    ROUND(
        COUNT(DISTINCT CASE WHEN review_score = 1 THEN order_id END) * 100.0 
        / NULLIF(COUNT(DISTINCT order_id), 0),
        2
    ) AS one_star_percentage,
    
    -- Total Negative Reviews
    MAX(n_negative_reviews) AS total_negative_reviews,
    
    -- Total Positive Reviews
    MAX(n_positive_reviews) AS total_positive_reviews,
    
    -- Positive Review Rate
    ROUND(
        MAX(n_positive_reviews) * 100.0 / NULLIF(COUNT(DISTINCT order_id), 0),
        2
    ) AS positive_review_rate,
    
    -- Negative Review Rate
    ROUND(
        MAX(n_negative_reviews) * 100.0 / NULLIF(COUNT(DISTINCT order_id), 0),
        2
    ) AS negative_review_rate,
    
    -- Average Review Response Time (days)
    ROUND(AVG(time_to_review), 1) AS avg_review_response_time_days,
    
    -- Reviews by Sentiment
    COUNT(DISTINCT CASE WHEN review_sentiment = 'Positive' THEN order_id END) AS positive_sentiment_count,
    COUNT(DISTINCT CASE WHEN review_sentiment = 'Neutral' THEN order_id END) AS neutral_sentiment_count,
    COUNT(DISTINCT CASE WHEN review_sentiment = 'Negative' THEN order_id END) AS negative_sentiment_count,
    
    -- Timestamp
    CURRENT_TIMESTAMP AS calculated_at

FROM review_data