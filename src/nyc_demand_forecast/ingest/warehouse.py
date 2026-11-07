"""Register landed files in the DuckDB warehouse's ``raw`` schema (the dbt sources).

Trip Parquet is exposed as a *view* over the files (zero-copy; DuckDB scans Parquet lazily and
pushes projections/filters down), so the 90M-row raw layer never has to fit in RAM. Small
reference data (zones, weather) is copied into tables.
"""

from __future__ import annotations

from pathlib import Path

from nyc_demand_forecast import config
from nyc_demand_forecast.ingest.tlc import month_range


def _sql_path(p: Path) -> str:
    return p.resolve().as_posix().replace("'", "''")


def trip_files(months: list[str] | None = None) -> list[Path]:
    months = months or month_range(config.CONFIG.start_month, config.CONFIG.end_month)
    files = [config.TLC_DIR / f"yellow_tripdata_{m}.parquet" for m in months]
    missing = [f.name for f in files if not f.exists()]
    if missing:
        raise FileNotFoundError(f"missing raw files (run ingestion first): {missing}")
    return files


def register_trips(con) -> int:  # noqa: ANN001
    """(Re)create the raw.yellow_tripdata view over the monthly files; returns its row count."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    file_list = ", ".join(f"'{_sql_path(f)}'" for f in trip_files())
    con.execute(
        f"""
        CREATE OR REPLACE VIEW raw.yellow_tripdata AS
        SELECT * FROM read_parquet([{file_list}], union_by_name = true, filename = true)
        """
    )
    return con.execute("SELECT count(*) FROM raw.yellow_tripdata").fetchone()[0]


def register_zones(con) -> int:  # noqa: ANN001
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    zones = _sql_path(config.TLC_DIR / "taxi_zone_lookup.csv")
    con.execute(
        f"CREATE OR REPLACE TABLE raw.taxi_zone_lookup AS "
        f"SELECT * FROM read_csv('{zones}', header = true, auto_detect = true)"
    )
    return con.execute("SELECT count(*) FROM raw.taxi_zone_lookup").fetchone()[0]


def register_weather(con) -> dict[str, int]:  # noqa: ANN001
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    counts = {}
    for name in ("observed", "forecast_d2"):
        src = _sql_path(config.WEATHER_DIR / f"weather_{name}.parquet")
        con.execute(f"CREATE OR REPLACE TABLE raw.weather_{name} AS SELECT * FROM read_parquet('{src}')")
        counts[f"raw.weather_{name}"] = con.execute(f"SELECT count(*) FROM raw.weather_{name}").fetchone()[0]
    return counts
