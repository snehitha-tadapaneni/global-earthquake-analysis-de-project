{{ config(materialized='table') }}

with location_data as (
    select distinct place
    from {{ ref('stg_earthquakes') }}
    where place is not null
)
select
    -- generate a surrogate key
    md5(cast(place as varchar)) as location_id,
    place,
    -- standardizing categories: split place to get general region
    trim(split_part(place, ' of ', 2)) as region
from location_data
