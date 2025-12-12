{{config(
    materializaed='view'
)}}

WITH logistics_data AS (
    SELECT *
    FROM {{ref('vm_delivery_performance')}}
)

SELECT
    'Delivery & Logistics Metrics' AS kpi_category,

    --TOTAL DELIVERIES
    COUNT(DISTINCT order_id) AS total_deliveries,

    --Average Delivery Time
    ROUND(AVG(actual_delivery_time), 1) AS avg_delivery_time_days,

    --Median Delivery time
    ROUND(MEDIAN(actual_delivery_time) , 1) AS median_delivery_time_days,
     
    --on time delivery percentage
    ROUND(
        COUNT(DISTINCT CASE WHEN delivery_status = 'On Time' THEN order_id END) * 100.0
        / NULLIF(COUNT(DISTINCT order_id), 0),
        2
    ) AS on_time_delivery_percentage,

    -- delayed delivery percentage
    ROUND(
        COUNT(DISTINCT CASE WHEN delivery_status = 'Delayed' THEN order_id END) * 100.0
        / NULLIF(COUNT(DISTINCT order_id), 0),
        2
    ) AS delayed_delivery_percentage, 

    --DElay rate
    ROUND(
        COUNT(DISTINCT CASE WHEN delivery_status IN ('Delayed', 'Overdue') THEN order_id END) * 100.0
        / NULLIF(COUNT(DISTINCT order_id), 0), 

        2
    ) AS total_delayed_rate,

    --Average Delay

    ROUND(AVG(CASE WHEN delivery_status = 'Delayed' THEN ABS(estimated_vs_actual_difference) END),
        1) AS avg_delay_days,


    --Maximum Delay
    MAX(CASE WHEN delivery_status= 'Delayed' THEN ABS(estimated_vs_actual_difference) END) AS max_delay_days,


    CURRENT_TIMESTAMP AS calculated_at

FROM logistics_data
WHERE delivery_timestamp IS NOT NULL