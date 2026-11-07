"""Generate the US federal holiday dbt seed from the ``holidays`` package (observed dates)."""

from __future__ import annotations

import csv
from pathlib import Path

import holidays

from nyc_demand_forecast import config

SEED_PATH = config.DBT_PROJECT_DIR / "seeds" / "us_federal_holidays.csv"


def write_holiday_seed(years: range = range(2023, 2028), path: Path = SEED_PATH) -> int:
    """Write (holiday_date, holiday_name) rows; returns the number of rows written."""
    cal = holidays.country_holidays("US", years=list(years), categories=("public",), observed=True)
    rows = sorted((d.isoformat(), name) for d, name in cal.items())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["holiday_date", "holiday_name"])
        w.writerows(rows)
    return len(rows)


if __name__ == "__main__":
    print(write_holiday_seed())
