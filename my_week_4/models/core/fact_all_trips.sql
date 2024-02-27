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
external_fhv_tripdata as (
    select *, 
    'FHV' as service_type 
    from {{ ref('stg_external_fhv_tripdata') }}
),
trips_unioned as (
    select tripid,
    service_type,
    pickup_locationid,
    dropoff_locationid,
    pickup_datetime,
    dropoff_datetime,
    from external_green_tripdata
    union all
    select tripid,
    service_type,
    pickup_locationid,
    dropoff_locationid,
    pickup_datetime,
    dropoff_datetime,
    from external_yellow_tripdata
    union all
    select tripid,
    service_type,
    pickup_locationid,
    dropoff_locationid,
    pickup_datetime,
    dropoff_datetime,
    from external_fhv_tripdata
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select trips_unioned.tripid, 
    trips_unioned.service_type,
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    
from trips_unioned 
inner join dim_zones as pickup_zone 
on trips_unioned.pickup_locationid = pickup_zone.locationid 
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid
