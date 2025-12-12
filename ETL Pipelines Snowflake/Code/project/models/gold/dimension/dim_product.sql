
{{
    config(
        materialized='table',
        unique_key='product_key'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    (product_length_cm * product_height_cm * product_width_cm) AS product_volume_cm3,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM {{ source('silver', 'olist_products_dataset') }}