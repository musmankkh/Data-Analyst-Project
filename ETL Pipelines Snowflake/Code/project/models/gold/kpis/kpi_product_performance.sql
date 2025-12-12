
{{
    config(
        materialized='view'
    )
}}

WITH product_data AS (
    SELECT * FROM {{ref("vm_product_performance") }}
),

top_10_products AS (
    SELECT 
        product_id,
        product_name,
       
        total_sales,
        ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS rank
    FROM product_data
    WHERE total_sales > 0
    ORDER BY total_sales DESC
    LIMIT 10
)

SELECT
    'Product Metrics' AS kpi_category,
    
    -- Total Products
    COUNT(DISTINCT product_id) AS total_products,
    
    -- Total Product Categories
    COUNT(DISTINCT product_name) AS total_categories,
    
    -- Top 10 Products (comma-separated names)
    (
        SELECT LISTAGG(product_name, ', ') 
               WITHIN GROUP (ORDER BY total_sales DESC)
        FROM top_10_products
    ) AS top_10_product_categories,
    
    -- Top Product Name
    (
        SELECT product_name
        FROM top_10_products
        WHERE rank = 1
    ) AS top_1_product_name,
    
    -- Top Product Revenue
    (
        SELECT total_sales
        FROM top_10_products
        WHERE rank = 1
    ) AS top_1_product_revenue,
    
    -- Best Performing Category (by revenue)
    (
        SELECT product_name
        FROM product_data
        WHERE product_name IS NOT NULL
        GROUP BY product_name
        ORDER BY SUM(total_sales) DESC
        LIMIT 1
    ) AS best_performing_category,
    
    -- Total Category Revenue (best category)
    (
        SELECT SUM(total_sales)
        FROM product_data
        WHERE product_name = (
            SELECT product_name
            FROM product_data
            WHERE product_name IS NOT NULL
            GROUP BY product_name
            ORDER BY SUM(total_sales) DESC
            LIMIT 1
        )
    ) AS best_category_revenue,
    
    -- Average Product Rating
    ROUND(AVG(avg_review_score), 2) AS avg_product_rating,
    
    -- Products with 5-star Average
    COUNT(DISTINCT CASE WHEN avg_review_score = 5 THEN product_id END) AS five_star_products,
    
    -- Products with Low Ratings (<3)
    COUNT(DISTINCT CASE WHEN avg_review_score < 3 THEN product_id END) AS low_rated_products,
    

    -- Average Selling Price
    ROUND(AVG(avg_selling_price), 2) AS avg_selling_price,
    
    -- Total Units Sold
    SUM(total_units_sold) AS total_units_sold,
    
    -- Products without Sales
    COUNT(DISTINCT CASE WHEN total_sales = 0 THEN product_id END) AS products_without_sales,
    
    -- Timestamp
    CURRENT_TIMESTAMP AS calculated_at

FROM product_data