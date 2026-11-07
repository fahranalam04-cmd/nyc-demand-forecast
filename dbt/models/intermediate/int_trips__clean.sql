-- Trips that passed every quality rule (one row per distinct record).
select
    pickup_at,
    pickup_zone_id,
    trip_distance_mi,
    fare_amount,
    total_amount,
    duration_min,
    source_month
from {{ ref('int_trips__validated') }}
where failed_rule is null
