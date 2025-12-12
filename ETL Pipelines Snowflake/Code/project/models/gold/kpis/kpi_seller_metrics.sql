
{{
    config(
        materialized='view'
    )
}}

WITH seller_data AS (
    SELECT * FROM {{ ref('vm_seller_performance')}}
)

SELECT
    'Seller Metrics' AS kpi_category,
    
    -- Total Sellers
    COUNT(DISTINCT seller_id) AS total_sellers,
    
    -- Active Sellers (with orders)
    COUNT(DISTINCT CASE WHEN total_orders_fulfilled > 0 THEN seller_id END) AS active_sellers,
    
    -- Average Seller Rating
    ROUND(AVG(avg_seller_review_score), 2) AS avg_seller_rating,
    
    -- Top Rated Sellers (rating >= 4.5)
    COUNT(DISTINCT CASE WHEN avg_seller_review_score >= 4.5 THEN seller_id END) AS top_rated_sellers,
    
    -- Average On-time Delivery %
    ROUND(AVG(on_time_delivery_percentage), 2) AS avg_on_time_delivery_percentage,
    
    -- Sellers with >90% On-time Delivery
    COUNT(DISTINCT CASE WHEN on_time_delivery_percentage > 90 THEN seller_id END) AS excellent_delivery_sellers,
    
    -- Total Seller Revenue
    ROUND(SUM(total_revenue_generated), 2) AS total_seller_revenue,
    
    -- Average Revenue per Seller
    ROUND(AVG(total_revenue_generated), 2) AS avg_revenue_per_seller,
    
    -- Seller Contribution to Total Sales (Top Seller)
    ROUND(
        MAX(total_revenue_generated) * 100.0 / NULLIF(SUM(total_revenue_generated), 0),
        2
    ) AS top_seller_contribution_percentage,
    
    -- Average Seller Fulfillment Speed (days)
    ROUND(AVG(avg_delivery_time_from_seller), 1) AS avg_seller_fulfillment_speed,
    
    -- Average Freight Cost per Order
    ROUND(AVG(freight_cost_per_order), 2) AS avg_freight_cost_per_order,
    
    -- Total Orders Fulfilled by Sellers
    SUM(total_orders_fulfilled) AS total_orders_fulfilled,
    
    -- Average Orders per Seller
    ROUND(AVG(total_orders_fulfilled), 2) AS avg_orders_per_seller,
    
    -- Total Units Sold by Sellers
    SUM(total_units_sold) AS total_units_sold,
    
    -- Timestamp
    CURRENT_TIMESTAMP AS calculated_at

FROM seller_data