{{config(
    materialized='table',
    unique_key='geolocation_key'
)}}


SELECT DISTINCT 
    {{dbt_utils.generate_surrogate_key(['geolocation_zip_code_prefix', 'geolocation_city', 'geolocation_state'])}} AS geolocation_key,
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{source('silver', 'olist_geolocation_dataset')}}    