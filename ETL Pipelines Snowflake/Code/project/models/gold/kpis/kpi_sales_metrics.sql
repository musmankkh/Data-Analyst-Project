
{{
    config(
        materialized='view'
    )
}}

WITH sales_data AS (
    SELECT * FROM {{ref('vm_sales_orders_fact')}}
),

current_month_sales AS (
    SELECT
        SUM(payment_value) AS current_month_revenue,
        COUNT(DISTINCT order_id) AS current_month_orders
    FROM sales_data
    WHERE DATE_TRUNC('month', order_purchase_timestamp) = DATE_TRUNC('month', CURRENT_DATE)
),

previous_month_sales AS (
    SELECT
        SUM(payment_value) AS previous_month_revenue,
        COUNT(DISTINCT order_id) AS previous_month_orders
    FROM sales_data
    WHERE DATE_TRUNC('month', order_purchase_timestamp) = DATE_TRUNC('month', DATEADD(month, -1, CURRENT_DATE))
),

current_year_sales AS (
    SELECT
        SUM(payment_value) AS current_year_revenue
    FROM sales_data
    WHERE EXTRACT(year FROM order_purchase_timestamp) = EXTRACT(year FROM CURRENT_DATE)
),

previous_year_sales AS (
    SELECT
        SUM(payment_value) AS previous_year_revenue
    FROM sales_data
    WHERE EXTRACT(year FROM order_purchase_timestamp) = EXTRACT(year FROM CURRENT_DATE) - 1
)

SELECT
    'Sales & Orders Metrics' AS kpi_category,
    
    -- Total Sales Revenue
    ROUND(SUM(sd.payment_value), 2) AS total_sales_revenue,
    
    -- Total Orders
    COUNT(DISTINCT sd.order_id) AS total_orders,
    
    -- Average Order Value (AOV)
    ROUND(AVG(sd.payment_value), 2) AS average_order_value,
    
    -- Sales Growth MoM
    ROUND(
        ((SELECT current_month_revenue FROM current_month_sales) - 
         (SELECT previous_month_revenue FROM previous_month_sales)) * 100.0 
        / NULLIF((SELECT previous_month_revenue FROM previous_month_sales), 0),
        2
    ) AS sales_growth_mom_percentage,
    
    -- Sales Growth YoY
    ROUND(
        ((SELECT current_year_revenue FROM current_year_sales) - 
         (SELECT previous_year_revenue FROM previous_year_sales)) * 100.0 
        / NULLIF((SELECT previous_year_revenue FROM previous_year_sales), 0),
        2
    ) AS sales_growth_yoy_percentage,
    
    -- Gross Margin (Total)
    ROUND(SUM(sd.contribution_margin), 2) AS total_gross_margin,
    
    -- Gross Margin Percentage
    ROUND(
        SUM(sd.contribution_margin) * 100.0 / NULLIF(SUM(sd.payment_value), 0),
        2
    ) AS gross_margin_percentage,
    
    -- Freight Cost % of Sales
    ROUND(
        SUM(sd.freight_value) * 100.0 / NULLIF(SUM(sd.payment_value), 0),
        2
    ) AS freight_cost_percentage,
    
    -- Total Freight Cost
    ROUND(SUM(sd.freight_value), 2) AS total_freight_cost,
    
    -- Return/Cancelled Order Rate
    ROUND(
        COUNT(DISTINCT CASE WHEN sd.order_status IN ('cancelled', 'unavailable') THEN sd.order_id END) * 100.0 
        / NULLIF(COUNT(DISTINCT sd.order_id), 0),
        2
    ) AS return_cancelled_rate,
    
    -- Average Items per Order
    ROUND(AVG(sd.item_count), 2) AS avg_items_per_order,
    
    -- Total Items Sold
    SUM(sd.item_count) AS total_items_sold,
    
    -- Current Month Revenue
    (SELECT current_month_revenue FROM current_month_sales) AS current_month_revenue,
    
    -- Current Month Orders
    (SELECT current_month_orders FROM current_month_sales) AS current_month_orders,
    
    -- Timestamp
    CURRENT_TIMESTAMP AS calculated_at

FROM sales_data sd