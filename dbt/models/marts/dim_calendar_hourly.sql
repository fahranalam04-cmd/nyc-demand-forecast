-- One row per NYC local wall-clock hour in the study window, with calendar, holiday and DST
-- attributes. All of these are known in advance, so they are legal day-ahead features.
{% set window_start = "timestamp '" ~ var('start_month') ~ "-01'" %}
{% set window_end = "(timestamp '" ~ var('end_month') ~ "-01' + interval 1 month)" %}

with local_hours as (
    select cast(range as timestamp) as hour_ts
    from range({{ window_start }}, {{ window_end }}, interval 1 hour)
),

-- How many real (UTC) hours map onto each local hour: 0 = spring-forward gap, 2 = fall-back repeat.
utc_hours_per_local as (
    select {{ to_nyc_local('cast(range as timestamp)') }} as hour_ts, count(*) as n_utc_hours
    from range(
        {{ window_start }} - interval 1 day,
        {{ window_end }} + interval 1 day,
        interval 1 hour
    )
    group by 1
),

holidays as (
    select holiday_date, string_agg(holiday_name, ' / ') as holiday_name
    from {{ ref('us_federal_holidays') }}
    group by 1
)

select
    h.hour_ts,
    cast(h.hour_ts as date)                           as date_day,
    extract('hour' from h.hour_ts)::integer           as hour_of_day,
    extract('isodow' from h.hour_ts)::integer         as day_of_week,  -- 1 = Monday
    extract('isodow' from h.hour_ts) in (6, 7)        as is_weekend,
    hol.holiday_date is not null                      as is_holiday,
    hol.holiday_name,
    coalesce(u.n_utc_hours, 0)                        as n_utc_hours,
    coalesce(u.n_utc_hours, 0) = 0                    as is_dst_gap_hour,
    coalesce(u.n_utc_hours, 0) = 2                    as is_dst_repeat_hour
from local_hours as h
left join utc_hours_per_local as u using (hour_ts)
left join holidays as hol on cast(h.hour_ts as date) = hol.holiday_date
