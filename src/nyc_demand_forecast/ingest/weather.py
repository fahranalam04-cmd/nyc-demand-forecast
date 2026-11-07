"""Hourly NYC weather from the free, keyless Open-Meteo APIs.

Two products are landed, both requested in GMT (Open-Meteo applies one fixed UTC offset to a whole
response, which is wrong across DST changes, so the local-time conversion is done in dbt):

* ``observed``  - Historical Weather API (reanalysis). What actually happened; used for the weather
  dimension and drift monitoring. Using it as a model feature would leak the future.
* ``forecast_d2`` - Previous Runs API, ``*_previous_day2`` variables: the value predicted by the
  model run ~48 h before the target hour. Every target hour of a day-ahead forecast issued at
  midnight is 1-24 h after the origin, so a run made 48 h before the target was issued 24-47 h
  before the origin -> available at forecast time, no leakage.
"""

from __future__ import annotations

import calendar
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from nyc_demand_forecast import config

log = logging.getLogger(__name__)

OBSERVED_VARS = ["temperature_2m", "precipitation", "snowfall", "wind_speed_10m"]
FORECAST_VARS = [f"{v}_previous_day2" for v in OBSERVED_VARS]


def window_dates(start_month: str, end_month: str) -> tuple[str, str]:
    """First day of start_month (minus a 1-day pad for the UTC->local shift) .. last day of end_month
    (plus a 1-day pad)."""
    sy, sm = map(int, start_month.split("-"))
    ey, em = map(int, end_month.split("-"))
    start = pd.Timestamp(sy, sm, 1) - pd.Timedelta(days=1)
    end = pd.Timestamp(ey, em, calendar.monthrange(ey, em)[1]) + pd.Timedelta(days=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def fetch_open_meteo(url: str, variables: list[str], start: str, end: str) -> pd.DataFrame:
    params = {
        "latitude": config.WEATHER_LAT,
        "longitude": config.WEATHER_LON,
        "start_date": start,
        "end_date": end,
        "hourly": ",".join(variables),
        "timezone": "GMT",
    }
    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()
    payload = resp.json()
    hourly = payload["hourly"]
    df = pd.DataFrame(hourly)
    df["time_utc"] = pd.to_datetime(df.pop("time"))
    df = df[["time_utc", *variables]]
    df.attrs["grid"] = {"latitude": payload.get("latitude"), "longitude": payload.get("longitude")}
    return df


def ingest_weather(force: bool = False) -> dict[str, Any]:
    config.ensure_dirs()
    start, end = window_dates(config.CONFIG.start_month, config.CONFIG.end_month)
    manifest_path = config.WEATHER_DIR / "weather_manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}

    products = {
        "observed": (config.OPEN_METEO_ARCHIVE_URL, OBSERVED_VARS),
        "forecast_d2": (config.OPEN_METEO_PREVIOUS_RUNS_URL, FORECAST_VARS),
    }
    for name, (url, variables) in products.items():
        dest: Path = config.WEATHER_DIR / f"weather_{name}.parquet"
        prev = manifest.get(name, {})
        if not force and dest.exists() and prev.get("start") == start and prev.get("end") == end:
            log.info("skip weather %s (already covers %s..%s)", name, start, end)
            manifest[name] = {**prev, "last_action": "skipped"}
            continue
        df = fetch_open_meteo(url, variables, start, end)
        df.to_parquet(dest, index=False)
        manifest[name] = {
            "file": dest.name,
            "url": url,
            "variables": variables,
            "start": start,
            "end": end,
            "rows": len(df),
            "null_cells": {v: int(df[v].isna().sum()) for v in variables},
            "grid": df.attrs.get("grid"),
            "downloaded_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "last_action": "downloaded",
        }
        manifest_path.write_text(json.dumps(manifest, indent=2))
    manifest_path.write_text(json.dumps(manifest, indent=2))
    return manifest
