-- Observed (reanalysis) weather on the NYC local-hour grid. The fall-back DST hour maps two UTC
-- hours onto one local hour, so values are averaged (precip/snow summed) per local hour.
select
    {{ to_nyc_local('time_utc') }}     as hour_ts,
    avg(temperature_2m)                as temp_c,
    sum(precipitation)                 as precip_mm,
    sum(snowfall)                      as snowfall_cm,
    avg(wind_speed_10m)                as wind_kmh
from {{ source('raw', 'weather_observed') }}
group by 1
