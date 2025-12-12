{{config(
    materialized='table'
)}}

WITH source_data AS (
    SELECT     
        "seller_id" AS seller_id,
        "seller_zip_code_prefix" AS seller_zip_code_prefix,
        "seller_city" AS seller_city,
        "seller_state" AS seller_state
    FROM {{source('bronze', 'olist_sellers_dataset')}}
),

--  STEP 1: Basic Cleaning & Type Conversion
basic_cleaning AS (
    SELECT
        -- Clean seller_id: trim whitespace
        TRIM(seller_id) AS seller_id,
        
        -- Convert ZIP to string and pad with leading zeros if needed
        LPAD(CAST(seller_zip_code_prefix AS VARCHAR), 5, '0') AS seller_zip_code_prefix,
        
        -- Normalize city: lowercase, trim, remove extra spaces
        LOWER(
            REGEXP_REPLACE(
                TRIM(seller_city), 
                '\s+', ' '  -- Replace multiple spaces with single space
            )
        ) AS seller_city_normalized,
        
        -- Normalize state: uppercase, trim
        UPPER(TRIM(seller_state)) AS seller_state_normalized
    FROM source_data
    --  Filter out completely null records
    WHERE seller_id IS NOT NULL 
        AND seller_zip_code_prefix IS NOT NULL
        AND seller_city IS NOT NULL
        AND seller_state IS NOT NULL
),

--  STEP 2: Remove Duplicates (by seller_id)
deduplicate_sellers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY seller_id
            ORDER BY seller_zip_code_prefix  -- Keep first ZIP if duplicates exist
        ) AS seller_id_row_num
    FROM basic_cleaning
),

--  STEP 3: Remove Duplicate ZIP Codes (keep first seller per location)
deduplicate_locations AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY seller_zip_code_prefix
            ORDER BY seller_id
        ) AS zip_row_num
    FROM deduplicate_sellers
    WHERE seller_id_row_num = 1  -- Only unique sellers
),

-- STEP 4: Data Validation & Quality Flags
quality_checks AS (
    SELECT
        *,
        
        -- Flag invalid ZIP codes (not 5 digits or out of range)
        CASE 
            WHEN LENGTH(seller_zip_code_prefix) != 5 THEN TRUE
            WHEN CAST(seller_zip_code_prefix AS INTEGER) < 1000 THEN TRUE
            WHEN CAST(seller_zip_code_prefix AS INTEGER) > 99999 THEN TRUE
            ELSE FALSE
        END AS is_invalid_zip,
        
        -- Flag invalid state codes (not 2 characters or not in valid list)
        CASE 
            WHEN LENGTH(seller_state_normalized) != 2 THEN TRUE
            WHEN seller_state_normalized NOT IN (
                'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 
                'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 
                'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
            ) THEN TRUE
            ELSE FALSE
        END AS is_invalid_state,
        
        -- Flag empty strings that passed NULL check
        CASE 
            WHEN TRIM(seller_id) = '' THEN TRUE
            WHEN TRIM(seller_city_normalized) = '' THEN TRUE
            ELSE FALSE
        END AS is_empty_string

    FROM deduplicate_locations
    WHERE zip_row_num = 1  -- Only unique ZIP codes
),

-- STEP 5: Final Clean Dataset
final AS (
    SELECT
        seller_id,
        seller_zip_code_prefix,
        seller_state_normalized AS seller_state,
        seller_city_normalized AS seller_city,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
        
    FROM quality_checks
    WHERE NOT (is_invalid_zip OR is_invalid_state OR is_empty_string)  -- Only keep valid records
)

SELECT * FROM final