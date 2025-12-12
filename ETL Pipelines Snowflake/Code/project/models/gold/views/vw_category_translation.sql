
{{
    config(
        materialized='view'
    )
}}

WITH category_performance AS (
    SELECT
        dp.product_category_name,
        SUM(fs.total_payment_value) AS total_revenue,
        SUM(fs.item_count) AS total_units,
        AVG(fr.review_score) AS avg_rating,
        SUM(fs.contribution_margin) AS category_profitability
    FROM {{ ref('facts_sales') }} fs
    JOIN {{ ref('dim_product') }} dp ON fs.product_key = dp.product_key
    LEFT JOIN {{ ref('facts_reviews') }} fr ON fs.order_id = fr.order_id
    GROUP BY dp.product_category_name
)

SELECT
    product_category_name,
    total_revenue,
    total_units,
    avg_rating,
    category_profitability
FROM category_performance
ORDER BY total_revenue DESC