{{ config(
    materialized='table',
) }}

WITH source_products AS (
    SELECT "product_id" AS product_id,
    "product_category_name" AS product_category_name,
    "product_name_lenght" as product_name_lenght,
    "product_description_lenght" AS product_description_lenght,
    "product_photos_qty" AS product_photos_qty,
    "product_weight_g" AS product_weight_g,
    "product_length_cm" AS product_length_cm,
    "product_height_cm" AS product_height_cm,
    "product_width_cm" AS product_width_cm,
    FROM {{ source('bronze', 'olist_products_dataset') }}
),

source_translations AS (
    SELECT "product_category_name" AS product_category_name,
    "product_category_name_english" AS product_category_name_english
    FROM {{ source('bronze', 'product_category_name_translation') }}
),

-- Clean and standardize translations (trim whitespace, lowercase)
cleaned_translations AS (
    SELECT 
        TRIM(LOWER(product_category_name)) AS product_category_name,
        product_category_name_english
    FROM source_translations
),

-- Remove duplicates by keeping first occurrence
deduped_products AS (
    SELECT 
        product_id,
        TRIM(LOWER(product_category_name)) AS product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY product_id
        ) AS row_num
    FROM source_products
    WHERE product_id IS NOT NULL
),

-- Join with translations
products_with_translation AS (
    SELECT 
        p.product_id,
        p.product_category_name AS product_category_name_portuguese,
        t.product_category_name_english,
        p.product_name_lenght,
        p.product_description_lenght,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    FROM deduped_products p
    LEFT JOIN cleaned_translations t
        ON p.product_category_name = t.product_category_name
    WHERE p.row_num = 1
),

-- Calculate category-level statistics for imputation
category_stats AS (
    SELECT
        COALESCE(product_category_name_english, product_category_name_portuguese, 'uncategorized') AS category,
        AVG(product_weight_g) AS avg_weight,
        AVG(product_length_cm) AS avg_length,
        AVG(product_height_cm) AS avg_height,
        AVG(product_width_cm) AS avg_width,
        AVG(product_name_lenght) AS avg_name_length,
        AVG(product_description_lenght) AS avg_desc_length,
        AVG(product_photos_qty) AS avg_photos
    FROM products_with_translation
    WHERE product_weight_g IS NOT NULL
      AND product_length_cm IS NOT NULL
      AND product_height_cm IS NOT NULL
      AND product_width_cm IS NOT NULL
    GROUP BY category
),

-- Apply imputation and cleaning logic
cleaned_products AS (
    SELECT
        p.product_id,
        COALESCE(
            p.product_category_name_english, 
            p.product_category_name_portuguese, 
            'uncategorized'
        ) AS product_category_name,
        
        -- Impute missing values using category averages
        COALESCE(
            p.product_name_lenght,
            cs.avg_name_length,
            (SELECT AVG(product_name_lenght) FROM products_with_translation WHERE product_name_lenght IS NOT NULL)
        ) AS product_name_length_raw,
        
        COALESCE(
            p.product_description_lenght,
            cs.avg_desc_length,
            (SELECT AVG(product_description_lenght) FROM products_with_translation WHERE product_description_lenght IS NOT NULL)
        ) AS product_description_length_raw,
        
        COALESCE(p.product_photos_qty, 0) AS product_photos_qty_raw,
        
        COALESCE(
            p.product_weight_g,
            cs.avg_weight,
            (SELECT AVG(product_weight_g) FROM products_with_translation WHERE product_weight_g IS NOT NULL)
        ) AS product_weight_g_raw,
        
        COALESCE(
            p.product_length_cm,
            cs.avg_length,
            (SELECT AVG(product_length_cm) FROM products_with_translation WHERE product_length_cm IS NOT NULL)
        ) AS product_length_cm_raw,
        
        COALESCE(
            p.product_height_cm,
            cs.avg_height,
            (SELECT AVG(product_height_cm) FROM products_with_translation WHERE product_height_cm IS NOT NULL)
        ) AS product_height_cm_raw,
        
        COALESCE(
            p.product_width_cm,
            cs.avg_width,
            (SELECT AVG(product_width_cm) FROM products_with_translation WHERE product_width_cm IS NOT NULL)
        ) AS product_width_cm_raw
        
    FROM products_with_translation p
    LEFT JOIN category_stats cs
        ON COALESCE(p.product_category_name_english, p.product_category_name_portuguese, 'uncategorized') = cs.category
),

-- Handle outliers and invalid values
final AS (
    SELECT
        -- Primary key
        product_id,
        
        -- Category
        product_category_name,
        
        -- Descriptive attributes (corrected column names, converted to integers)
        CAST(product_name_length_raw AS INTEGER) AS product_name_length,
        CAST(product_description_length_raw AS INTEGER) AS product_description_length,
        CAST(product_photos_qty_raw AS INTEGER) AS product_photos_qty,
        
        -- Physical dimensions with outlier handling
        -- Cap weight between 10g and 50,000g (50kg)
        CAST(
            CASE 
                WHEN product_weight_g_raw < 10 THEN 10
                WHEN product_weight_g_raw > 50000 THEN 50000
                ELSE product_weight_g_raw
            END AS DECIMAL(10,2)
        ) AS product_weight_g,
        
        -- Cap dimensions at 200cm and ensure > 0
        CAST(
            CASE 
                WHEN product_length_cm_raw <= 0 THEN 1
                WHEN product_length_cm_raw > 200 THEN 200
                ELSE product_length_cm_raw
            END AS DECIMAL(10,2)
        ) AS product_length_cm,
        
        CAST(
            CASE 
                WHEN product_height_cm_raw <= 0 THEN 1
                WHEN product_height_cm_raw > 200 THEN 200
                ELSE product_height_cm_raw
            END AS DECIMAL(10,2)
        ) AS product_height_cm,
        
        CAST(
            CASE 
                WHEN product_width_cm_raw <= 0 THEN 1
                WHEN product_width_cm_raw > 200 THEN 200
                ELSE product_width_cm_raw
            END AS DECIMAL(10,2)
        ) AS product_width_cm,
        
        -- Calculate product volume (cubic cm)
        CAST(
            (CASE WHEN product_length_cm_raw <= 0 THEN 1 WHEN product_length_cm_raw > 200 THEN 200 ELSE product_length_cm_raw END) *
            (CASE WHEN product_height_cm_raw <= 0 THEN 1 WHEN product_height_cm_raw > 200 THEN 200 ELSE product_height_cm_raw END) *
            (CASE WHEN product_width_cm_raw <= 0 THEN 1 WHEN product_width_cm_raw > 200 THEN 200 ELSE product_width_cm_raw END)
            AS DECIMAL(15,2)
        ) AS product_volume_cm3,
        
        -- Data quality flags
        CASE 
            WHEN product_category_name = 'uncategorized' THEN TRUE 
            ELSE FALSE 
        END AS is_uncategorized,
        
        CASE 
            WHEN product_weight_g_raw < 10 OR product_weight_g_raw > 50000 
              OR product_length_cm_raw <= 0 OR product_length_cm_raw > 200
              OR product_height_cm_raw <= 0 OR product_height_cm_raw > 200
              OR product_width_cm_raw <= 0 OR product_width_cm_raw > 200
            THEN TRUE 
            ELSE FALSE 
        END AS has_outlier_dimensions,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_loaded_at
        
    FROM cleaned_products
)

SELECT * FROM final