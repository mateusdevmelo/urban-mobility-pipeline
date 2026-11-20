-- mv_monthly_taxi_metrics.sql
-- Materialized View analítica com os indicadores mensais agregados por frotista

{{ config(
    materialized='materialized_view'
) }}

with fact as (
    select * from {{ ref('fct_taxi_trips') }}
),

vendors as (
    select * from {{ ref('dim_vendor') }}
),

aggregated as (
    select
        f.pickup_year_month,
        f.vendor_id,
        v.vendor_name,
        count(*) as total_trips,
        round(sum(case when f.is_valid_trip and f.is_valid_payment then f.total_amount else 0 end)::numeric, 2) as total_valid_revenue,
        round(avg(case when f.is_valid_trip and f.is_valid_payment then f.total_amount else null end)::numeric, 2) as avg_ticket_per_trip,
        round(avg(case when f.is_valid_trip then f.trip_distance else null end)::numeric, 2) as avg_trip_distance,
        current_timestamp as last_refreshed_at
    from fact f
    left join vendors v on f.vendor_id = v.vendor_id
    group by f.pickup_year_month, f.vendor_id, v.vendor_name
)

select * from aggregated
order by pickup_year_month, vendor_id
