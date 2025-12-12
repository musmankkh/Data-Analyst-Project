{{config(
    materialized='view'
)}}

WITH customer_metrics AS (
    SELECT 
        fs.customer_key,
        COUNT(DISTINCT fs.order_id) AS total_orders,
        SUM(fs.total_payment_value) AS total_revenue,
        AVG(fs.total_payment_value) AS avg_order_value,
        SUM(fs.item_count) AS total_items_ordered,
        SUM(fs.contribution_margin) AS total_contribution_margin
    FROM {{ref('facts_sales')}} fs  
    GROUP BY fs.customer_key

),

customer_reviews AS (
    SELECT 
        {{dbt_utils.generate_surrogate_key(['o.customer_id'])}} AS customer_key,
        AVG(fr.review_score) AS avg_review_score
    FROM    {{ref("facts_reviews")}} fr
    JOIN {{source('silver', 'olist_orders_dataset')}} o
    ON fr.order_id = o.order_id
    GROUP BY customer_key   
),

customer_logistics AS (

    SELECT 
        customer_key,
        AVG(total_delivery_days) AS avg_delivery_time
    FROM {{ref('facts_logistics')}}
    GROUP BY customer_key    
)

SELECT
    dc.customer_key,
    dc.customer_id,
    dc.customer_unique_id,
    dc.customer_city,
    dc.customer_state,
    dc.first_order_date,
    dc.last_order_date,
    dc.customer_status,
    dc.customer_tenure_days,


    COALESCE(cm.total_orders, 0) AS total_orders,
    COALESCE(cm.avg_order_value, 0) AS avg_order_value,
    COALESCE(cm.total_revenue, 0) AS total_revenue,
    COALESCE(cm.total_items_ordered, 0) AS total_items_ordered,
    COALESCE(cm.total_contribution_margin, 0) AS customer_lifetime_value,
    COALESCE(cl.avg_delivery_time, 0) AS avg_delivery_time,
    COALESCE(cr.avg_review_score, 0) AS avg_review_score,

    --customer segmentation

    CASE 
        WHEN cm.total_orders >= 5  THEN 'Loyal'
        WHEN cm.total_orders >= 2 THEN 'Repeat'
        ELSE 'One-Time'
    END AS customer_segment

FROM {{ref('dim_customer')}} dc
LEFT JOIN customer_metrics cm ON dc.customer_key = cm.customer_key
LEFT JOIN customer_reviews cr ON dc.customer_key = cr.customer_key
LEFT JOIN customer_logistics cl ON dc.customer_key = cl.customer_key