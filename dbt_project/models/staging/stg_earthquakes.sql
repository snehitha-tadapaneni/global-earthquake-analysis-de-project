{{ config(materialized='view') }}

with source as (
    select * from {{ source('public', 'raw_earthquakes') }}
)
select
    id as earthquake_id,
    cast(mag as double precision) as magnitude,
    place,
    -- Extract time correctly: USGS time is unix time in milliseconds
    to_timestamp(time / 1000.0) as earthquake_time,
    to_timestamp(updated / 1000.0) as updated_time,
    cast(longitude as double precision) as longitude,
    cast(latitude as double precision) as latitude,
    cast(depth as double precision) as depth_km,
    cast(magType as varchar) as magnitude_type,
    cast(type as varchar) as event_type,
    sig as significance
from source
-- Basic deduplication from raw table just in case incremental logic failed
qualify row_number() over (partition by id order by updated desc) = 1
