{{ config(
    materialized='table'
) }}

WITH source_data AS (
    SELECT 
        "review_id" AS review_id,
        "order_id" AS order_id,
        "review_score" AS review_score,
        "review_comment_title" AS review_comment_title,
        "review_comment_message" AS review_comment_message,
        "review_creation_date" AS review_creation_date,
        "review_answer_timestamp" AS review_answer_timestamp
    FROM {{ source('bronze', 'olist_order_reviews_dataset') }}
),

-- Step 1: Basic validation and type conversion
validated_data AS (
    SELECT 
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        
        -- Convert date strings to timestamps
        TRY_CAST(review_creation_date AS TIMESTAMP) as review_creation_date,
        TRY_CAST(review_answer_timestamp AS TIMESTAMP) as review_answer_timestamp
        
    FROM source_data
    WHERE review_id IS NOT NULL 
      AND order_id IS NOT NULL
      AND review_score IS NOT NULL
),

-- Step 2: Deduplicate by keeping most recent review
deduplicated_data AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY review_id 
            ORDER BY review_creation_date DESC NULLS LAST,
                     review_answer_timestamp DESC NULLS LAST
        ) as row_num
    FROM validated_data
    WHERE review_creation_date IS NOT NULL
      AND review_score BETWEEN 1 AND 5
),

-- Step 3: Clean and transform
final AS (
    SELECT 
        -- Primary Keys
        review_id,
        order_id,
        
        -- Review Score (validated 1-5)
        CAST(review_score AS INTEGER) as review_score,
        
        -- Text Fields - Standardize missing values to NULL
        CASE 
            WHEN review_comment_title IS NULL THEN NULL
            WHEN TRIM(review_comment_title) = '' THEN NULL
            WHEN LOWER(TRIM(review_comment_title)) IN ('na', 'n/a', 'null', 'none') THEN NULL
            ELSE TRIM(review_comment_title)
        END as review_comment_title,
        
        CASE 
            WHEN review_comment_message IS NULL THEN NULL
            WHEN TRIM(review_comment_message) = '' THEN NULL
            WHEN LOWER(TRIM(review_comment_message)) IN ('na', 'n/a', 'null', 'none') THEN NULL
            ELSE TRIM(review_comment_message)
        END as review_comment_message,
        
        -- Date Fields
        review_creation_date,
        review_answer_timestamp,
        
        -- Metadata
        CURRENT_TIMESTAMP() as dbt_loaded_at
        
    FROM deduplicated_data
    WHERE row_num = 1  -- Keep only one record per review_id
)

SELECT * FROM final