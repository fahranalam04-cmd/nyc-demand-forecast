"""Pull the modelling frame out of the warehouse (aggregated in DuckDB, ~0.9M rows in pandas).

Series: the top-N zones by clean volume (one ``zones`` group, modelled by one global model) plus a
``citywide`` total over all modelable zones (modelled separately: it is ~50x larger than any zone
and would dominate a pooled loss).

Exogenous columns are all *known at forecast time*: calendar/holiday flags, and the day-2-ahead
weather forecast (never observed weather).
"""

from __future__ import annotations

import pandas as pd

from nyc_demand_forecast import config

WEATHER_COLS = ["fcst_temp_c", "fcst_precip_mm", "fcst_snowfall_cm", "fcst_wind_kmh"]
CALENDAR_COLS = ["is_holiday", "is_holiday_adjacent"]
EXOG_COLS = CALENDAR_COLS + WEATHER_COLS
CITYWIDE_ID = "citywide"

SERIES_SQL = """
with top_zones as (
    select zone_id from marts.dim_zones where volume_rank <= {top_n}
),
series as (
    select 'z' || lpad(cast(zone_id as varchar), 3, '0') as unique_id, hour_ts as ds, pickups as y,
           'zones' as series_group
    from marts.fct_hourly_zone_demand
    where zone_id in (select zone_id from top_zones)
    union all
    select '{citywide}', hour_ts, sum(pickups), 'citywide'
    from marts.fct_hourly_zone_demand
    group by hour_ts
)
select s.unique_id, s.ds, cast(s.y as double) as y, s.series_group,
       cast(c.is_holiday as integer) as is_holiday,
       w.fcst_temp_c, w.fcst_precip_mm, w.fcst_snowfall_cm, w.fcst_wind_kmh
from series as s
join marts.dim_calendar_hourly as c on c.hour_ts = s.ds
join marts.dim_weather_hourly as w on w.hour_ts = s.ds
order by s.unique_id, s.ds
"""


def load_series(top_n: int | None = None, con=None) -> pd.DataFrame:
    """Return the long-format modelling frame (unique_id, ds, y, series_group, exog...)."""
    top_n = top_n or config.CONFIG.top_n_zones
    own = con is None
    con = con or config.duckdb_connect(read_only=True)
    try:
        df = con.execute(SERIES_SQL.format(top_n=int(top_n), citywide=CITYWIDE_ID)).df()
    finally:
        if own:
            con.close()
    df["ds"] = pd.to_datetime(df["ds"])
    # Day before / after a federal holiday (Thanksgiving Friday, Christmas Eve, ...): per *day*.
    hol_days = set(df.loc[df["is_holiday"] == 1, "ds"].dt.normalize())
    day = df["ds"].dt.normalize()
    adjacent = day.sub(pd.Timedelta(days=1)).isin(hol_days) | day.add(pd.Timedelta(days=1)).isin(hol_days)
    df["is_holiday_adjacent"] = (adjacent & (df["is_holiday"] == 0)).astype("int8")
    df["is_holiday"] = df["is_holiday"].astype("int8")
    return df


def validate_series(df: pd.DataFrame) -> dict[str, int | bool]:
    """Structural checks used by the Dagster asset check and the tests."""
    per_series = df.groupby("unique_id")["ds"].agg(["min", "max", "count"])
    expected = ((per_series["max"] - per_series["min"]) / pd.Timedelta(hours=1) + 1).astype(int)
    return {
        "n_series": int(df["unique_id"].nunique()),
        "n_rows": len(df),
        "regular_hourly_index": bool((per_series["count"] == expected).all()),
        "same_span_all_series": bool(per_series["min"].nunique() == 1 and per_series["max"].nunique() == 1),
        "null_cells": int(df[["y", *EXOG_COLS]].isna().sum().sum()),
        "negative_targets": int((df["y"] < 0).sum()),
    }
