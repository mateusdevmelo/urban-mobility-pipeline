-- dim_date.sql
-- Dimensão de calendário a partir das datas de embarque das viagens

{{ config(materialized='table') }}

with dates as (
    select distinct
        pickup_date as date_key,
        extract(year from pickup_date)::integer as year,
        extract(month from pickup_date)::integer as month,
        extract(day from pickup_date)::integer as day,
        extract(quarter from pickup_date)::integer as quarter,
        to_char(pickup_date, 'YYYYMM') as year_month,
        extract(isodow from pickup_date)::integer as day_of_week,
        trim(to_char(pickup_date, 'Day')) as day_name,
        case when extract(isodow from pickup_date) in (6, 7) then true else false end as is_weekend
    from {{ ref('stg_yellow_taxi_trips') }}
    where pickup_date is not null
)

select * from dates
order by date_key
