-- Audit of vendor-level reporting quirks found while profiling the raw feed. These explain why
-- the quality rules are narrower than the obvious "duration <= 0 or amount < 0" filter:
--   * vendor 7 (first seen 2024-12) reports dropoff = pickup on every record;
--   * vendor 2 Flex Fare trips (payment_type 0) carried negative fare_amount with a positive
--     total through 2025-11.
-- A naive filter would delete these real pickups and inject level shifts into the target.
select
    source_month,
    vendor_id,
    count(*)                                                                  as rows_raw,
    count(*) filter (where dropoff_at = pickup_at)                            as zero_duration,
    count(*) filter (where dropoff_at = pickup_at and trip_distance_mi > 0)   as zero_duration_with_distance,
    count(*) filter (where fare_amount < 0 and total_amount >= 0)             as negative_fare_positive_total,
    count(*) filter (where total_amount < 0)                                  as negative_total,
    count(*) filter (where payment_type = 0)                                  as payment_type_0
from {{ ref('stg_tlc__yellow_trips') }}
group by 1, 2
order by 1, 2
