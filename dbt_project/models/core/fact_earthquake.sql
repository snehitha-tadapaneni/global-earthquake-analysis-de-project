{{ config(materialized='table') }}

with stg_earth as (
    select * from {{ ref('stg_earthquakes') }}
),
dim_loc as (
    select * from {{ ref('dim_location') }}
)
select
    e.earthquake_id,
    e.magnitude,
    e.earthquake_time,
    e.earthquake_time::date as date_id,
    l.location_id,
    e.longitude,
    e.latitude,
    e.depth_km,
    e.magnitude_type,
    e.event_type,
    e.significance
from stg_earth e
left join dim_loc l on e.place = l.place
