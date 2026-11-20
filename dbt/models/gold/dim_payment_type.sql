-- dim_payment_type.sql
-- Dimensão de modalidades de pagamento baseada na tabela oficial de referência

{{ config(materialized='table') }}

with payments as (
    select
        payment_type,
        payment_description,
        is_valid_payment
    from {{ ref('stg_payment_types') }}
)

select * from payments
