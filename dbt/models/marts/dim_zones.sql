-- Modelable taxi zones with their total clean pickup volume over the study window and a
-- volume rank (1 = busiest), which the forecasting step uses to pick the top-N series.
with volume as (
    select pickup_zone_id as zone_id, count(*) as clean_pickups
    from {{ ref('int_trips__clean') }}
    group by 1
)

select
    z.zone_id,
    z.borough,
    z.zone_name,
    z.service_zone,
    coalesce(v.clean_pickups, 0) as clean_pickups,
    row_number() over (order by coalesce(v.clean_pickups, 0) desc, z.zone_id) as volume_rank
from {{ ref('stg_tlc__taxi_zones') }} as z
left join volume as v using (zone_id)
where z.is_modelable
