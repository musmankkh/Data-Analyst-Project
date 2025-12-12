{{config(
    materialized='view'
)}}

WITH customer_data AS (
    SELECT * FROM {{ref('vw_customer_360')}}
),

time_periods AS (
    SELECT  
        CURRENT_DATE AS today,
        DATEADD(month, -12, CURRENT_DATE) AS twelve_month_ago,
        DATEADD(month, -1, CURRENT_DATE) AS one_month_ago

)

SELECT 
    'Customer Metrics' AS kpi_category,
    COUNT(DISTINCT customer_id) AS total_customers,
    COUNT(DISTINCT CASE
    WHEN last_order_date >= (SELECT twelve_month_ago FROM time_periods)
    THEN customer_id
    END) AS active_customers_12m,
    
    ROUND(
        COUNT(DISTINCT CASE WHEN last_order_date >= (SELECT twelve_month_ago FROM time_periods) THEN customer_id END) *100.0
        / NULLIF(COUNT(DISTINCT customer_id), 0), 
        2
    ) AS active_customers_percentage,

    ROUND(AVG(customer_lifetime_value),2) AS avg_customer_lifetime_value,
    ROUND(SUM(customer_lifetime_value), 2) AS total_customer_lifetime_value,
    ROUND(MEDIAN(customer_lifetime_value), 2) AS median_customer_lifetime_value,
        ROUND(
        COUNT(DISTINCT CASE WHEN total_orders > 1 THEN customer_id END) * 100.0 
        / NULLIF(COUNT(DISTINCT customer_id), 0),
        2
    ) AS repeat_purchase_rate,

    --CUSTOMER CHRUN RATE (INACTIVE)
    ROUND(
        COUNT(DISTINCT CASE WHEN customer_status = 'Inactive' THEN customer_id END) * 100.0 
        / NULLIF(COUNT(DISTINCT customer_id), 0),
        2
    ) AS customer_churn_rate,

    --AVERAGE ORDER PER CUSTOMER
    ROUND(AVG(total_orders), 2) AS avg_orders_per_customer,

    -- AVERAGE ORDER VALUE
    ROUND(AVG(avg_order_value), 2) AS avg_order_value,
    
    -- Average Customer Tenure (days)
    ROUND(AVG(customer_tenure_days), 0) AS avg_customer_tenure_days,

    COUNT(DISTINCT CASE WHEN customer_segment = 'Loyal' THEN customer_id END) AS loyal_customers,
    COUNT(DISTINCT CASE WHEN customer_segment = 'Repeat' THEN customer_id END) AS repeat_customers,
    COUNT(DISTINCT CASE WHEN customer_segment = 'One-Time' THEN customer_id END) AS one_time_customers,
    
    -- Average Delivery Time
    ROUND(AVG(avg_delivery_time), 1) AS avg_delivery_time_days,
    
    -- Average Review Score
    ROUND(AVG(avg_review_score), 2) AS avg_customer_review_score,
    
    -- Timestamp
    CURRENT_TIMESTAMP AS calculated_at

FROM customer_data
