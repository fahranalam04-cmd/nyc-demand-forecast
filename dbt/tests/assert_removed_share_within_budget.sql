-- Data-quality budget: no month may lose more than 10% of its raw rows to quality rules.
select source_month, rows_raw, rows_clean, 1 - rows_clean / rows_raw as removed_share
from {{ ref('rpt_trip_quality') }}
where 1 - rows_clean / rows_raw > 0.10
