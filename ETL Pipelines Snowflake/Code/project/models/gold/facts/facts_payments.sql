{{config(
    materialized='table',
    unique_key='payment_key'
)}}

SELECT  
    {{dbt_utils.generate_surrogate_key(['order_id', 'payment_sequential'])}} AS payment_key,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    CASE 
        WHEN payment_installments > 0 THEN payment_value/payment_installments
        ELSE payment_value
    END AS installment_amount,
    CURRENT_TIMESTAMP AS created_at
FROM {{source('silver', 'olist_order_payments_dataset')}}        