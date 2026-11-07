{{ config(materialized='table') }}

-- Deduplicate, then tag each distinct record with the FIRST quality rule it fails (rules are
-- evaluated in a fixed order so removed-row counts add up exactly; see rpt_trip_quality).
-- Materialised once (~90M rows) so the demand fact and the quality report don't each re-scan
-- and re-deduplicate the raw Parquet.
with deduplicated as (
    select
        vendor_id, pickup_at, dropoff_at, passenger_count, trip_distance_mi,
        pickup_zone_id, dropoff_zone_id, payment_type, fare_amount, total_amount, source_month,
        count(*) as n_copies  -- > 1 means exact duplicate records in the source file
    from {{ ref('stg_tlc__yellow_trips') }}
    group by all
),

modelable_zones as (
    select zone_id from {{ ref('stg_tlc__taxi_zones') }} where is_modelable
)

select
    d.vendor_id,
    d.pickup_at,
    d.pickup_zone_id,
    d.trip_distance_mi,
    d.fare_amount,
    d.total_amount,
    -- Unknown (null) when the dropoff clock is not usable, e.g. vendor 7 reports dropoff = pickup.
    case when d.dropoff_at > d.pickup_at
        then date_diff('second', d.pickup_at, d.dropoff_at) / 60.0 end as duration_min,
    d.source_month,
    d.n_copies,
    case
        when d.pickup_at is null
            or d.pickup_at < d.source_month
            or d.pickup_at >= d.source_month + interval 1 month
            then 'pickup_outside_file_month'
        -- dropoff == pickup with a real distance is a broken dropoff clock (all of vendor 7's
        -- records), not a fake trip: the pickup itself is valid demand, so it is kept.
        when d.dropoff_at is null
            or d.dropoff_at < d.pickup_at
            or d.dropoff_at > d.pickup_at + interval {{ var('max_trip_hours') }} hour
            or (d.dropoff_at = d.pickup_at and coalesce(d.trip_distance_mi, 0) = 0)
            then 'invalid_duration'
        -- Negative TOTAL = refund / void adjustment row. A negative fare_amount with a positive
        -- total is the Flex Fare (payment_type 0) reporting convention and is a real trip.
        when d.total_amount < 0
            then 'negative_amount'
        when z.zone_id is null
            then 'unknown_pickup_zone'
        when d.trip_distance_mi is null
            or d.trip_distance_mi < 0
            or d.trip_distance_mi > {{ var('max_trip_miles') }}
            then 'implausible_distance'
    end as failed_rule
from deduplicated as d
left join modelable_zones as z
    on d.pickup_zone_id = z.zone_id
