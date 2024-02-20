{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    fhv_tripdata.dispatching_base_num as dispatching_base_num,
    fhv_tripdata.pickup_datetime as pickup_datetime,
    fhv_tripdata.dropoff_datetime as dropoff_datetime,
    fhv_tripdata.pulocationid,
    fhv_tripdata.dolocationid,
    fhv_tripdata.sr_flag,
    fhv_tripdata.affiliated_base_number,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone, 
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dolocationid = dropoff_zone.locationid
