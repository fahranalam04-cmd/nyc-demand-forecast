-- Dense hourly pickup demand: every modelable zone x every local hour in the window, with zero
-- for hours without a trip. Grain: (zone_id, hour_ts). This is the forecasting target table.
select
    z.zone_id,
    z.borough,
    c.hour_ts,
    coalesce(d.pickups, 0)::integer as pickups,
    d.avg_trip_distance_mi,
    d.avg_fare_amount
from {{ ref('dim_zones') }} as z
cross join {{ ref('dim_calendar_hourly') }} as c
left join {{ ref('int_demand__zone_hour_counts') }} as d
    on d.zone_id = z.zone_id and d.hour_ts = c.hour_ts
