{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(

    select * from {{ source('staging', 'fhv_tripdata_2019') }}
    where pulocationid is not null and dolocationid is not null
)
select
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    pulocationid,
    dolocationid,
    sr_flag,
    affiliated_base_number
from tripdata
where pickup_datetime between '2019-01-01 00:00:00' and '2019-12-12 23:59:59'
 
-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}
  limit 100

{% endif %}