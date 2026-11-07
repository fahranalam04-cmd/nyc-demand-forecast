-- Guards the UTC -> NYC local conversion: the day-2 temperature forecast must track observed
-- temperature (MAE < 3 C) and be best aligned at lag 0. A one-hour time-zone slip would make a
-- shifted comparison score better.
with w as (
    select
        hour_ts, temp_c, fcst_temp_c,
        lag(fcst_temp_c, 1) over (order by hour_ts) as fcst_lag1,
        lead(fcst_temp_c, 1) over (order by hour_ts) as fcst_lead1
    from {{ ref('dim_weather_hourly') }}
),
mae as (
    select
        avg(abs(fcst_temp_c - temp_c)) as mae_0,
        avg(abs(fcst_lag1 - temp_c)) as mae_lag1,
        avg(abs(fcst_lead1 - temp_c)) as mae_lead1
    from w
)
select * from mae
where mae_0 >= 3 or mae_lag1 < mae_0 or mae_lead1 < mae_0
