-- Volume anomaly check: month-over-month change in clean trips beyond +/-35% signals a broken or
-- partial file (normal seasonality in this window is well inside that band).
with m as (
    select source_month, rows_clean,
           lag(rows_clean) over (order by source_month) as prev_clean
    from {{ ref('rpt_trip_quality') }}
)
select source_month, rows_clean, prev_clean, rows_clean / prev_clean - 1 as pct_change
from m
where prev_clean is not null and abs(rows_clean / prev_clean - 1) > 0.35
