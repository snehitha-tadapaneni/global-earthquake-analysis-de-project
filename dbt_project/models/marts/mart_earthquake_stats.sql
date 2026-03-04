{{ config(materialized='table') }}

with fact_earth as (
    select * from {{ ref('fact_earthquake') }}
)
select
    date_id,
    event_type,
    count(earthquake_id) as total_events,
    max(magnitude) as max_magnitude,
    avg(magnitude) as avg_magnitude,
    avg(depth_km) as avg_depth_km,
    sum(case when magnitude >= 5.0 then 1 else 0 end) as significant_events,
    max(significance) as max_significance
from fact_earth
group by 1, 2
