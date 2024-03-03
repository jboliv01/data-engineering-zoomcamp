{{
    config(
        materialized='table'
    )
}}

with external_fhv_tripdata as (
    select *, 
    'FHV' as service_type 
    from {{ ref('stg_external_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

SELECT 
external_fhv_tripdata.dispatching_base_num,
external_fhv_tripdata.affiliated_base_number,
external_fhv_tripdata.pickup_locationid,
external_fhv_tripdata.dropoff_locationid,
external_fhv_tripdata.pickup_datetime,
external_fhv_tripdata.dropoff_datetime,
external_fhv_tripdata.service_type
FROM external_fhv_tripdata
inner join dim_zones as pickup_zone 
on external_fhv_tripdata.pickup_locationid = pickup_zone.locationid 
inner join dim_zones as dropoff_zone
on external_fhv_tripdata.dropoff_locationid = dropoff_zone.locationid