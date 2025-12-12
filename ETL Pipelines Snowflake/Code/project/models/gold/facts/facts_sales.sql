{{config(
    materializated='table',
    unique_key='sales_key'
)}}

WITH order_items_agg AS (
    SELECT 
        oi.order_id,
        oi.product_id,
        oi.seller_id,
        SUM(oi.price) AS total_price,
        SUM(oi.freight_value) AS total_freight,
        COUNT(*) AS item_count
    FROM {{source('silver', 'olist_order_items_dataset')}} oi   
    GROUP BY oi.order_id , oi.product_id, oi.seller_id
),

order_payments_agg AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment_value
    FROM {{source('silver', 'olist_order_payments_dataset')}} op
    GROUP BY order_id    
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['o.order_id', 'oia.product_id', 'oia.seller_id']) }} AS sales_key,
    o.order_id,
    {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }} AS customer_key,
    {{ dbt_utils.generate_surrogate_key(['oia.seller_id']) }} AS seller_key,
    {{ dbt_utils.generate_surrogate_key(['oia.product_id']) }} AS product_key,
    CAST(TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD') AS INTEGER) AS order_date_key,
    
    --order details
    o.order_purchase_timestamp,
    o.order_status,

    --financial metrics
    oia.total_price,
    oia.total_freight,
    COALESCE(opa.total_payment_value, 0) AS total_payment_value,
    (oia.total_price - oia.total_freight) AS contribution_margin,
    oia.item_count,

    CURRENT_TIMESTAMP AS created_at
FROM {{source('silver', 'olist_orders_dataset')}} o
INNER JOIN order_items_agg oia ON o.order_id = oia.order_id
LEFT JOIN order_payments_agg opa ON o.order_id = opa.order_id
WHERE o.order_status NOT IN ('cancelled','unavailable')