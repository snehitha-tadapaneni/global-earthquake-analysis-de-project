{{ config(materialized='table') }}

with date_spine as (
    select distinct 
        date_trunc('day', earthquake_time)::date as date_day
    from {{ ref('stg_earthquakes') }}
)
select
    date_day as date_id,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    extract(dow from date_day) as day_of_week
from date_spine
