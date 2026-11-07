-- Weather on the full local-hour spine. The spring-forward gap hour has no UTC hour, so it is
-- filled with the previous hour's value (carried forward, never from the future).
with joined as (
    select
        c.hour_ts,
        o.temp_c, o.precip_mm, o.snowfall_cm, o.wind_kmh,
        f.fcst_temp_c, f.fcst_precip_mm, f.fcst_snowfall_cm, f.fcst_wind_kmh
    from {{ ref('dim_calendar_hourly') }} as c
    left join {{ ref('stg_open_meteo__weather_observed') }} as o using (hour_ts)
    left join {{ ref('stg_open_meteo__weather_forecast') }} as f using (hour_ts)
)

select
    hour_ts,
    {% for col in ['temp_c', 'precip_mm', 'snowfall_cm', 'wind_kmh', 'fcst_temp_c', 'fcst_precip_mm', 'fcst_snowfall_cm', 'fcst_wind_kmh'] %}
    coalesce({{ col }}, last_value({{ col }} ignore nulls) over (
        order by hour_ts rows between unbounded preceding and 1 preceding
    )) as {{ col }}{{ ',' if not loop.last }}
    {% endfor %}
from joined
