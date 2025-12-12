
{{
    config(
        materialized='view'
    )
}}

WITH seller_sales AS (
    SELECT
        fs.seller_key,
        COUNT(DISTINCT fs.order_id) AS total_orders_fulfilled,
        SUM(fs.total_payment_value) AS total_revenue_generated,
        SUM(fs.item_count) AS total_units_sold,
        AVG(fs.total_freight) AS avg_freight_cost
    FROM {{ ref('facts_sales') }} fs
    GROUP BY fs.seller_key
),

seller_reviews AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['oi.seller_id']) }} AS seller_key,
        AVG(fr.review_score) AS avg_seller_review_score
    FROM {{ ref('facts_reviews') }} fr
    JOIN {{ source('silver', 'olist_order_items_dataset') }} oi ON fr.order_id = oi.order_id
    GROUP BY seller_key
),

seller_logistics AS (
    SELECT
        fs.seller_key,
        AVG(fl.total_delivery_days) AS avg_delivery_time,
        SUM(CASE WHEN fl.delivery_status = 'On Time' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS on_time_percentage
    FROM {{ ref('facts_sales') }} fs
    JOIN {{ ref('facts_logistics') }} fl ON fs.order_id = fl.order_id
    GROUP BY fs.seller_key
)

SELECT
    ds.seller_key,
    ds.seller_id,
    ds.seller_city,
    ds.seller_state,
    
    COALESCE(ss.total_orders_fulfilled, 0) AS total_orders_fulfilled,
    COALESCE(ss.total_revenue_generated, 0) AS total_revenue_generated,
    COALESCE(sr.avg_seller_review_score, 0) AS avg_seller_review_score,
    COALESCE(sl.avg_delivery_time, 0) AS avg_delivery_time_from_seller,
    COALESCE(ss.total_units_sold, 0) AS total_units_sold,
    COALESCE(ss.avg_freight_cost, 0) AS freight_cost_per_order,
    COALESCE(sl.on_time_percentage, 0) AS on_time_delivery_percentage
    
FROM {{ ref('dim_seller') }} ds
LEFT JOIN seller_sales ss ON ds.seller_key = ss.seller_key
LEFT JOIN seller_reviews sr ON ds.seller_key = sr.seller_key
LEFT JOIN seller_logistics sl ON ds.seller_key = sl.seller_key