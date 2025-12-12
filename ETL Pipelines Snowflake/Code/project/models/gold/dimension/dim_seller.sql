{{config(
    materialized='table',
    unique_key='seller_key'
)}}


SELECT 
    {{dbt_utils.generate_surrogate_key(['seller_id'])}} AS seller_key,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at,
FROM {{source('silver', 'olist_sellers_dataset')}}

