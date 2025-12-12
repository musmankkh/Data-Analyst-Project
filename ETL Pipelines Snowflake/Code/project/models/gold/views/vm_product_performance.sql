
{{
    config(
        materialized='view'
    )
}}

WITH product_sales AS (
    SELECT
        fs.product_key,
        SUM(fs.total_payment_value) AS total_sales,
        SUM(fs.item_count) AS total_units_sold,
        AVG(fs.total_price / NULLIF(fs.item_count, 0)) AS avg_selling_price,
        SUM(fs.contribution_margin) AS total_contribution_margin,
        COUNT(DISTINCT fs.order_id) AS total_orders
    FROM {{ ref('facts_sales') }} fs
    GROUP BY fs.product_key
),

product_reviews AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['oi.product_id']) }} AS product_key,
        AVG(fr.review_score) AS avg_review_score,
        COUNT(*) AS review_count
    FROM {{ ref('facts_reviews') }} fr
    JOIN {{ source('silver', 'olist_order_items_dataset') }} oi ON fr.order_id = oi.order_id
    GROUP BY product_key
)

-- product_returns AS (
--     SELECT
--         {{ dbt_utils.generate_surrogate_key(['oi.product_id']) }} AS product_key,
--         COUNT(*) AS returned_orders
--     FROM {{ source('silver', 'olist_orders_dataset') }} o
--     JOIN {{ source('silver', 'olist_order_items_dataset') }} oi ON o.order_id = oi.order_id
--     WHERE o.order_status IN ('cancelled', 'unavailable')
--     GROUP BY product_key
-- )

SELECT
    dp.product_key,
    dp.product_id,
    dp.product_category_name AS product_name,
   
    COALESCE(ps.total_sales, 0) AS total_sales,
    COALESCE(ps.total_units_sold, 0) AS total_units_sold,
    COALESCE(ps.avg_selling_price, 0) AS avg_selling_price,
    COALESCE(pr.avg_review_score, 0) AS avg_review_score,
    -- COALESCE(pret.returned_orders, 0) * 100.0 / NULLIF(ps.total_orders, 0) AS return_rate,
    COALESCE(ps.total_contribution_margin, 0) AS contribution_margin_per_product
    
FROM {{ ref('dim_product') }} dp
LEFT JOIN product_sales ps ON dp.product_key = ps.product_key
LEFT JOIN product_reviews pr ON dp.product_key = pr.product_key
-- LEFT JOIN product_returns pret ON dp.product_key = pret.product_key