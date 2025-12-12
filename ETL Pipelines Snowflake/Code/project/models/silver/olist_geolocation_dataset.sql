{{config(materialized='table')}}

WITH source_data AS (
    SELECT
        "geolocation_zip_code_prefix" AS geolocation_zip_code_prefix,
        "geolocation_lat" AS geolocation_lat,
        "geolocation_lng" AS geolocation_lng,
        "geolocation_city" AS geolocation_city,
        "geolocation_state" AS geolocation_state
    FROM {{ source('bronze', 'olist_geolocation_dataset') }}
    WHERE geolocation_zip_code_prefix IS NOT NULL
      AND geolocation_lat IS NOT NULL
      AND geolocation_lng IS NOT NULL
      AND geolocation_city IS NOT NULL
      AND geolocation_state IS NOT NULL
      AND TRIM(geolocation_city) != ''
      AND TRIM(geolocation_state) != ''
      -- Brazil-specific coordinate boundaries
      AND geolocation_lat::NUMBER(10,8) BETWEEN -34 AND 5
      AND geolocation_lng::NUMBER(11,8) BETWEEN -74 AND -34
),

cleaned_data AS (
    SELECT
        -- Convert ZIP code to string with zero-padding to preserve leading zeros
        LPAD(geolocation_zip_code_prefix::VARCHAR, 5, '0') AS geolocation_zip_code_prefix,
        geolocation_lat::NUMBER(10,8) AS geolocation_lat,
        geolocation_lng::NUMBER(11,8) AS geolocation_lng,
        -- Normalize city names: remove accents, trim whitespace, lowercase
        -- Enhanced normalization to handle all common Portuguese accents
        TRIM(
            LOWER(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(geolocation_city, '[áàâãä]', 'a', 1, 0, 'i'),
                                    '[éèêë]', 'e', 1, 0, 'i'
                                ),
                                '[íìîï]', 'i', 1, 0, 'i'
                            ),
                            '[óòôõö]', 'o', 1, 0, 'i'
                        ),
                        '[úùûü]', 'u', 1, 0, 'i'
                    ),
                    '[ç]', 'c', 1, 0, 'i'
                )
            )
        ) AS geolocation_city_normalized,
        -- Original city name (for reference)
        TRIM(geolocation_city) AS geolocation_city_original,
        -- Uppercase state codes
        TRIM(UPPER(geolocation_state)) AS geolocation_state
    FROM source_data
),

deduplicated AS (
    SELECT DISTINCT
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city_normalized,
        geolocation_city_original,
        geolocation_state
    FROM cleaned_data
),

-- Keep ONE representative row per ZIP prefix (median coordinates)
unique_zip AS (
    SELECT
        geolocation_zip_code_prefix,
        geolocation_city_normalized,
        geolocation_city_original,
        geolocation_state,
        -- Use median or mode for coordinates per ZIP
        MEDIAN(geolocation_lat) AS geolocation_lat,
        MEDIAN(geolocation_lng) AS geolocation_lng,
        COUNT(*) AS duplicate_count,
        ROW_NUMBER() OVER (
            PARTITION BY geolocation_zip_code_prefix
            ORDER BY COUNT(*) DESC, geolocation_zip_code_prefix
        ) AS rn
    FROM deduplicated
    GROUP BY 
        geolocation_zip_code_prefix,
        geolocation_city_normalized,
        geolocation_city_original,
        geolocation_state
),

final AS (
    SELECT
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        -- Final trim to ensure absolutely no whitespace
        TRIM(geolocation_city_normalized) AS geolocation_city,
        geolocation_city_original,
        geolocation_state,
        duplicate_count,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM unique_zip
    WHERE rn = 1
)

SELECT * FROM final