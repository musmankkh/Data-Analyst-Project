{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT 
        "order_id" AS order_id,
        "customer_id" AS customer_id,
        "order_status" AS order_status,
        "order_purchase_timestamp" AS order_purchase_timestamp,
        "order_approved_at" AS order_approved_at,
        "order_delivered_carrier_date" AS order_delivered_carrier_date,
        "order_delivered_customer_date" AS order_delivered_customer_date,
        "order_estimated_delivery_date" AS order_estimated_delivery_date
    FROM {{ source('bronze', 'olist_orders_dataset') }}
    -- Check ALL columns for NULL or empty strings
    WHERE order_id IS NOT NULL AND TRIM(order_id) != ''
      AND customer_id IS NOT NULL AND TRIM(customer_id) != ''
      AND order_status IS NOT NULL AND TRIM(order_status) != ''
      AND order_purchase_timestamp IS NOT NULL
      AND order_approved_at IS NOT NULL
      AND order_delivered_carrier_date IS NOT NULL
      AND order_delivered_customer_date IS NOT NULL
      AND order_estimated_delivery_date IS NOT NULL
),

-- Remove duplicates based on order_id
deduped_data AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_purchase_timestamp DESC
        ) AS row_num
    FROM source_data
),

-- Cast timestamps and standardize order_status
casted_data AS (
    SELECT 
        order_id,
        customer_id,
        TRIM(UPPER(order_status)) AS order_status,
        TRY_CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_timestamp,
        TRY_CAST(order_approved_at AS TIMESTAMP) AS order_approved_at,
        TRY_CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
        TRY_CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
        TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date
    FROM deduped_data
    WHERE row_num = 1
      -- Ensure all timestamp casts succeeded
      AND TRY_CAST(order_purchase_timestamp AS TIMESTAMP) IS NOT NULL
      AND TRY_CAST(order_approved_at AS TIMESTAMP) IS NOT NULL
      AND TRY_CAST(order_delivered_carrier_date AS TIMESTAMP) IS NOT NULL
      AND TRY_CAST(order_delivered_customer_date AS TIMESTAMP) IS NOT NULL
      AND TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) IS NOT NULL
),

-- Compute time deltas for validation only
time_deltas AS (
    SELECT *,
        DATEDIFF('day', order_purchase_timestamp, order_delivered_customer_date) AS delivery_time_days
    FROM casted_data
),

-- Calculate IQR boundaries for outlier detection
delivery_stats AS (
    SELECT
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY delivery_time_days) AS q1_delivery,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY delivery_time_days) AS q3_delivery,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY delivery_time_days) - 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY delivery_time_days) AS iqr_delivery
    FROM time_deltas
    WHERE delivery_time_days IS NOT NULL
),

-- Data quality validation with comprehensive checks
validated_data AS (
    SELECT 
        td.*,
        ds.q1_delivery,
        ds.q3_delivery,
        ds.iqr_delivery,
        
        -- Logical inconsistency flags
        CASE 
            WHEN order_delivered_customer_date < order_purchase_timestamp 
            THEN TRUE ELSE FALSE 
        END AS is_delivery_before_purchase,
        
        CASE 
            WHEN order_delivered_customer_date < order_delivered_carrier_date 
            THEN TRUE ELSE FALSE 
        END AS is_delivery_before_carrier,
        
        CASE 
            WHEN order_approved_at < order_purchase_timestamp 
            THEN TRUE ELSE FALSE 
        END AS is_approval_before_purchase,
        
        CASE 
            WHEN order_estimated_delivery_date < order_purchase_timestamp 
            THEN TRUE ELSE FALSE 
        END AS is_estimated_before_purchase,
        
        -- Status-specific validation
        CASE 
            WHEN order_status = 'DELIVERED' AND order_delivered_customer_date IS NULL 
            THEN TRUE ELSE FALSE 
        END AS is_invalid_delivered_status,
        
        CASE 
            WHEN order_status = 'SHIPPED' AND order_delivered_carrier_date IS NULL 
            THEN TRUE ELSE FALSE 
        END AS is_invalid_shipped_status,
        
        -- Outlier detection using IQR method (1.5 * IQR rule)
        CASE 
            WHEN delivery_time_days < (ds.q1_delivery - 1.5 * ds.iqr_delivery)
                 OR delivery_time_days > (ds.q3_delivery + 1.5 * ds.iqr_delivery)
            THEN TRUE ELSE FALSE 
        END AS is_delivery_time_outlier
        
    FROM time_deltas td
    CROSS JOIN delivery_stats ds
),

-- Final cleaned dataset (only original columns + metadata)
final AS (
    SELECT 
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM validated_data
    -- Exclude logically inconsistent records
    WHERE NOT is_delivery_before_purchase
      AND NOT is_delivery_before_carrier
      AND NOT is_approval_before_purchase
      AND NOT is_estimated_before_purchase
      AND NOT is_invalid_delivered_status
      AND NOT is_invalid_shipped_status
      AND NOT is_delivery_time_outlier
)

SELECT * FROM final