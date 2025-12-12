{{config(
    materialized='table',
    unique_key='customer_key')}}

WITH customer_base AS (
    SELECT 
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM {{source('silver', 'olist_customers_dataset')}}    
),


customer_orders AS (
    SELECT
        c.customer_id,
        MIN(o.order_purchase_timestamp) AS first_order_date,
        MAX(o.order_purchase_timestamp) AS last_order_date,
        COUNT(DISTINCT order_id) AS total_orders
    FROM {{source('silver', 'olist_orders_dataset')}} o
    JOIN   customer_base c ON   o.customer_id = c.customer_id
    WHERE o.order_status NOT IN ('cancelled', 'unavailable')
    GROUP BY c.customer_id
)

SELECT 
    {{dbt_utils.generate_surrogate_key(['cb.customer_id'])}} AS customer_key,
    cb.customer_id,
    cb.customer_unique_id,
    cb.customer_zip_code_prefix,
    customer_city,
    cb.customer_state,
    co.first_order_date,
    co.last_order_date,
    co.total_orders,
    DATEDIFF(day, co.first_order_date, co.last_order_date) AS customer_tenure_days,
    CASE
        WHEN co.last_order_date >= DATEADD(month, -12, CURRENT_DATE) THEN 'Active'
        ELSE 'Inactive'
    END AS customer_status,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at,
FROM customer_base cb
INNER JOIN customer_orders co
ON cb.customer_id = co.customer_id        