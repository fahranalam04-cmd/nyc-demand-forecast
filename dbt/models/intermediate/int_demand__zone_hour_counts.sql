-- Sparse hourly pickup counts (only zone-hours with >= 1 clean trip). Densified in the fact.
select
    pickup_zone_id                   as zone_id,
    date_trunc('hour', pickup_at)    as hour_ts,
    count(*)                         as pickups,
    avg(trip_distance_mi)            as avg_trip_distance_mi,
    avg(fare_amount)                 as avg_fare_amount
from {{ ref('int_trips__clean') }}
group by 1, 2
