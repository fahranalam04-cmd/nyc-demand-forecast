-- Day-2-ahead weather *forecasts* (issued ~48 h before each target hour) on the local-hour grid.
-- These, not the observations, are what the forecasting model may use as exogenous features.
select
    {{ to_nyc_local('time_utc') }}        as hour_ts,
    avg(temperature_2m_previous_day2)     as fcst_temp_c,
    sum(precipitation_previous_day2)      as fcst_precip_mm,
    sum(snowfall_previous_day2)           as fcst_snowfall_cm,
    avg(wind_speed_10m_previous_day2)     as fcst_wind_kmh
from {{ source('raw', 'weather_forecast_d2') }}
group by 1
