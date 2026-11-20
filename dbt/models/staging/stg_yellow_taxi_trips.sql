-- stg_yellow_taxi_trips.sql
-- Modelo de staging da camada Silver para a Silver/Gold do dbt

with source as (
    select * from {{ source('silver', 'yellow_taxi_trips') }}
),

renamed as (
    select
        trip_id,
        coalesce("VendorID", 0) as vendor_id,
        payment_type,
        pickup_date,
        pickup_datetime,
        dropoff_datetime,
        pickup_year_month,
        coalesce(passenger_count, 0)::integer as passenger_count,
        trip_distance,
        trip_duration_minutes,
        fare_amount,
        total_amount,
        valid_revenue_amount,
        is_valid_trip,
        is_valid_payment,
        is_anomaly,
        invalid_reason,
        anomaly_reason
    from source
)

select * from renamed
