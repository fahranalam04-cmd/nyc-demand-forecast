select
    cast(LocationID as integer)  as zone_id,
    cast(Borough as varchar)     as borough,
    cast(Zone as varchar)        as zone_name,
    cast(service_zone as varchar) as service_zone,
    -- 264 = "Unknown", 265 = "Outside of NYC": valid codes, but not places we can forecast.
    cast(LocationID as integer) not in ({{ var('unknown_zone_ids') | join(', ') }}) as is_modelable
from {{ source('raw', 'taxi_zone_lookup') }}
