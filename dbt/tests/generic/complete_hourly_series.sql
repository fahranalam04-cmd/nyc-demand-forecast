{#- Every partition must have exactly one row per hour between the global min and max timestamp
    (no gaps, no duplicates). Forecasting lags assume a regular hourly index. -#}
{% test complete_hourly_series(model, timestamp_column, partition_by) %}
with bounds as (
    select min({{ timestamp_column }}) as t0, max({{ timestamp_column }}) as t1 from {{ model }}
),
per_partition as (
    select {{ partition_by }}, count(*) as n_rows, count(distinct {{ timestamp_column }}) as n_hours
    from {{ model }}
    group by 1
)
select p.*
from per_partition as p, bounds as b
where p.n_rows != date_diff('hour', b.t0, b.t1) + 1
   or p.n_hours != p.n_rows
{% endtest %}
