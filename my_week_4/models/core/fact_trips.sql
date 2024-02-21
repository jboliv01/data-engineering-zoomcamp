{{
    config(
        materialized='table'
    )
}}

with external_green_tripdata as (
    select *, 
    'Green' as service_type 
    from {{ ref('stg_external_green_tripdata') }}
),
external_yellow_tripdata as (
    select *, 
    'Yellow' as service_type 
    from {{ ref('stg_external_yellow_tripdata') }}
),
trips_union as (
    select * from external_green_tripdata
    union all
    select * from external_yellow_tripdata
),
dim_zones as (
    select * from {{ ref('taxi_zone_lookup') }}
    where borough != 'Unknown'
)

select *
from trips_union 
inner join dim_zones as pickup_zone 
on trips_union.pickup_locationid = pickup_zone.locationid 
inner join dim_zones as dropoff_zone
on trips_union.dropoff_locationid = dropoff_zone.locationid
