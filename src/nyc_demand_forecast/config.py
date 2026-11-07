"""Central configuration: every path is read from env vars with portable defaults.

Heavy artefacts (raw Parquet, the DuckDB warehouse, MLflow runs) live outside the repo under
``$ML_DATA_ROOT/forecast`` and ``$ML_MODELS_ROOT/forecast``; the repo only holds code and small
reports.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

SLUG = "forecast"
REPO_ROOT = Path(__file__).resolve().parents[2]
REPORTS_DIR = REPO_ROOT / "reports"
FIGURES_DIR = REPORTS_DIR / "figures"
DBT_PROJECT_DIR = REPO_ROOT / "dbt"


def _env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return Path(value) if value else default


DATA_ROOT = _env_path("FORECAST_DATA_DIR", _env_path("ML_DATA_ROOT", Path("data")) / SLUG)
MODELS_ROOT = _env_path("FORECAST_MODELS_DIR", _env_path("ML_MODELS_ROOT", Path("models")) / SLUG)

RAW_DIR = DATA_ROOT / "raw"
TLC_DIR = RAW_DIR / "tlc"
WEATHER_DIR = RAW_DIR / "weather"
FEATURES_DIR = DATA_ROOT / "features"
DUCKDB_PATH = _env_path("FORECAST_DUCKDB_PATH", DATA_ROOT / "warehouse.duckdb")
DUCKDB_TEMP_DIR = DATA_ROOT / "duckdb_tmp"
MLFLOW_DB = MODELS_ROOT / "mlflow.db"
MLFLOW_ARTIFACTS = MODELS_ROOT / "mlartifacts"

# Resource etiquette: the laptop is shared, so DuckDB and LightGBM are capped.
DUCKDB_MEMORY_LIMIT = os.environ.get("FORECAST_DUCKDB_MEMORY_LIMIT", "3GB")
N_THREADS = int(os.environ.get("FORECAST_THREADS", "4"))

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
OPEN_METEO_PREVIOUS_RUNS_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
# Midtown Manhattan; one point is a reasonable proxy for city-wide weather.
WEATHER_LAT, WEATHER_LON = 40.7589, -73.9851


@dataclass(frozen=True)
class PipelineConfig:
    """Pinned study window and modelling knobs (pinned so reruns are reproducible)."""

    start_month: str = os.environ.get("FORECAST_START_MONTH", "2024-10")
    end_month: str = os.environ.get("FORECAST_END_MONTH", "2026-09")
    top_n_zones: int = 50
    horizon: int = 24  # day-ahead, hourly
    test_weeks: int = 12  # rolling-origin test folds (weekly refits)
    calib_weeks: int = 4  # warm-up weeks: forecasts only used to calibrate conformal intervals
    val_weeks: int = 4  # model-selection weeks, strictly before calibration + test
    interval_levels: tuple[int, ...] = (80, 95)
    seed: int = 42


CONFIG = PipelineConfig()


def ensure_dirs() -> None:
    for d in (TLC_DIR, WEATHER_DIR, FEATURES_DIR, DUCKDB_TEMP_DIR, MODELS_ROOT, REPORTS_DIR, FIGURES_DIR):
        d.mkdir(parents=True, exist_ok=True)


def duckdb_connect(read_only: bool = False):
    """Open the warehouse with the shared-laptop resource caps applied."""
    import duckdb

    ensure_dirs()
    con = duckdb.connect(str(DUCKDB_PATH), read_only=read_only)
    con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads={N_THREADS}")
    con.execute(f"SET temp_directory='{DUCKDB_TEMP_DIR.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    return con
