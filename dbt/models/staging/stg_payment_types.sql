-- stg_payment_types.sql
-- Modelo de staging da tabela de referência de pagamentos

with source as (
    select * from {{ source('silver', 'ref_payment_type') }}
),

extras as (
    select 0 as payment_type, 'Unknown' as payment_description, false as is_valid_payment
),

combined as (
    select payment_type, payment_description, is_valid_payment from source
    union all
    select payment_type, payment_description, is_valid_payment from extras
    where not exists (select 1 from source where source.payment_type = 0)
)

select * from combined
