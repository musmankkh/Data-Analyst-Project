{{ config(materialized='table') }}

WITH source_data AS (
  SELECT
    "customer_id"                AS customer_id,
    "customer_unique_id"         AS customer_unique_id,
    "customer_zip_code_prefix"   AS customer_zip_code_prefix,
    "customer_city"              AS customer_city,
    "customer_state"             AS customer_state
  FROM {{ source('bronze', 'olist_customers_dataset') }}
),

-- 1. BASIC CLEANING
standardized AS (
  SELECT
    customer_id,
    customer_unique_id,

    -- Convert zip prefix to text (categorical)
    CAST(customer_zip_code_prefix AS TEXT) AS customer_zip_code_prefix,

    -- Normalize city names
    TRIM(LOWER(customer_city)) AS customer_city_clean,

    -- Normalize state codes
    TRIM(UPPER(customer_state)) AS customer_state_clean
  FROM source_data
  WHERE customer_id IS NOT NULL
    AND customer_unique_id IS NOT NULL
),

-- 2. REMOVE DUPLICATE ROWS BASED ON CUSTOMER_ID
deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY customer_unique_id
    ) AS row_num
  FROM standardized
),

final AS (
  SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city_clean   AS customer_city,
    customer_state_clean  AS customer_state,
    CURRENT_TIMESTAMP()   AS dbt_loaded_at
  FROM deduped
  WHERE row_num = 1
)

SELECT * FROM final
