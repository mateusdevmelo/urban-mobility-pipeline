-- dim_vendor.sql
-- Dimensão com os dados dos frotistas / provedores autorizados do NYC TLC

{{ config(materialized='table') }}

with raw_vendors as (
    select distinct coalesce(vendor_id, 0) as vendor_id
    from {{ ref('stg_yellow_taxi_trips') }}
),

known as (
    select 1 as vendor_id, 'Creative Mobile Technologies, LLC' as vendor_name, 'CMT' as vendor_code
    union all
    select 2 as vendor_id, 'VeriFone Inc.' as vendor_name, 'VTS' as vendor_code
    union all
    select 0 as vendor_id, 'Unknown Vendor' as vendor_name, 'UNKNOWN' as vendor_code
)

select
    r.vendor_id,
    coalesce(k.vendor_name, 'Other Vendor (' || r.vendor_id || ')') as vendor_name,
    coalesce(k.vendor_code, 'V' || r.vendor_id) as vendor_code
from raw_vendors r
left join known k on r.vendor_id = k.vendor_id
