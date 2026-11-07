-- Every raw row is accounted for exactly once: removed as a duplicate, removed by one rule, or
-- kept. Also ties the report's total back to the raw source.
with per_month as (
    select
        source_month,
        rows_raw,
        removed_duplicate + removed_pickup_outside_file_month + removed_invalid_duration
            + removed_negative_amount + removed_unknown_pickup_zone + removed_implausible_distance
            + rows_clean as rows_accounted
    from {{ ref('rpt_trip_quality') }}
),
totals as (
    select
        (select sum(rows_raw) from {{ ref('rpt_trip_quality') }}) as report_raw,
        (select count(*) from {{ source('raw', 'yellow_tripdata') }}) as source_raw
)
select 'month' as level, cast(source_month as varchar) as key, rows_raw, rows_accounted
from per_month where rows_raw != rows_accounted
union all
select 'total', 'all', report_raw, source_raw from totals where report_raw != source_raw
