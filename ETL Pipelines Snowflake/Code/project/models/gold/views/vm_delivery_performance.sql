{{config(
    materialized='view'
)}}

SELECT 
    fl.order_id,
    fl.order_purchase_timestamp AS purchase_timestamp,
    fl.order_delivered_customer_date AS delivery_timestamp,
    fl.order_estimated_delivery_date AS estimated_delivery_date,
    fl.total_delivery_days AS actual_delivery_time,
    fl.delivery_vs_estimate_days AS estimated_vs_actual_difference,
    fl.delivery_status,

    0 AS distance_customer_seller
FROM {{ref('facts_logistics')}} fl    