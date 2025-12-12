
{{
    config(
        materialized='view'
    )
}}

SELECT
    fs.sales_key,
    fs.order_id,
    fs.order_purchase_timestamp,
    fs.order_status,
    
    -- Customer info
    dc.customer_id,
    dc.customer_city,
    dc.customer_state,
    
    -- Seller info
    ds.seller_id,
    ds.seller_city,
    ds.seller_state,
    
    -- Product info
    dp.product_id,

    
    -- Financial metrics
    fs.total_price AS price,
    fs.total_freight AS freight_value,
    fs.total_payment_value AS payment_value,
    fs.contribution_margin,
    fs.item_count,
    
    -- Logistics
    fl.total_delivery_days AS delivery_time,
    fl.delivery_status,
    
    -- Review
    COALESCE(fr.review_score, 0) AS review_score
    
FROM {{ ref('facts_sales') }} fs
LEFT JOIN {{ ref('dim_customer') }} dc ON fs.customer_key = dc.customer_key
LEFT JOIN {{ ref('dim_seller') }} ds ON fs.seller_key = ds.seller_key
LEFT JOIN {{ ref('dim_product') }} dp ON fs.product_key = dp.product_key
LEFT JOIN {{ ref('facts_logistics') }} fl ON fs.order_id = fl.order_id
LEFT JOIN {{ ref('facts_reviews') }} fr ON fs.order_id = fr.order_id