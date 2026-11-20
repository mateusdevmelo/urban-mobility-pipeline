-- fct_taxi_trips.sql
-- Tabela Fato de corridas de taxi contendo chaves dimensionais e métricas analíticas

{{ config(
    materialized='table',
    indexes=[
      {'columns': ['pickup_year_month']},
      {'columns': ['vendor_id']},
      {'columns': ['pickup_date']}
    ]
) }}

with trips as (
    select * from {{ ref('stg_yellow_taxi_trips') }}
),

vendors as (
    select * from {{ ref('dim_vendor') }}
),

payments as (
    select * from {{ ref('dim_payment_type') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

final as (
    select
        t.trip_id,
        coalesce(v.vendor_id, 0) as vendor_id,
        case
            when p.payment_type is not null then p.payment_type
            when t.payment_type in (1, 2, 3, 4, 5, 6, 0) then t.payment_type
            else 5
        end as payment_type,
        t.pickup_date,
        t.pickup_datetime,
        t.dropoff_datetime,
        t.pickup_year_month,
        t.passenger_count,
        round(t.trip_distance::numeric, 2) as trip_distance,
        round(t.trip_duration_minutes::numeric, 2) as trip_duration_minutes,
        round(t.fare_amount::numeric, 2) as fare_amount,
        round(t.total_amount::numeric, 2) as total_amount,
        round(t.valid_revenue_amount::numeric, 2) as valid_revenue_amount,
        t.is_valid_trip,
        t.is_valid_payment,
        t.is_anomaly,
        t.invalid_reason,
        t.anomaly_reason,
        current_timestamp as loaded_at
    from trips t
    left join vendors v on t.vendor_id = v.vendor_id
    left join payments p on t.payment_type = p.payment_type
    left join dates d on t.pickup_date = d.date_key
)

select * from final
