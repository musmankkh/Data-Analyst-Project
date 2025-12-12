-- models/gold/facts/fact_logistics.sql

{{
    config(
        materialized='table',
        unique_key='logistics_key'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS logistics_key,
    order_id,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS customer_key,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    
    -- Calculated delivery metrics
    DATEDIFF(hour, order_purchase_timestamp, order_approved_at) AS approval_time_hours,
    DATEDIFF(day, order_approved_at, order_delivered_carrier_date) AS carrier_processing_days,
    DATEDIFF(day, order_delivered_carrier_date, order_delivered_customer_date) AS delivery_days,
    DATEDIFF(day, order_purchase_timestamp, order_delivered_customer_date) AS total_delivery_days,
    DATEDIFF(day, order_delivered_customer_date, order_estimated_delivery_date) AS delivery_vs_estimate_days,
    
    -- Delivery status
    CASE 
        WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 'On Time'
        WHEN order_delivered_customer_date > order_estimated_delivery_date THEN 'Delayed'
        WHEN order_delivered_customer_date IS NULL AND order_estimated_delivery_date < CURRENT_DATE THEN 'Overdue'
        ELSE 'In Transit'
    END AS delivery_status,
    
    CURRENT_TIMESTAMP AS created_at
FROM {{ source('silver','olist_orders_dataset') }}
WHERE order_status = 'DELIVERED'