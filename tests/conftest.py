import numpy as np
import pandas as pd
import pytest


def make_series(n_series: int = 2, hours: int = 24 * 7 * 8, seed: int = 0) -> pd.DataFrame:
    """Synthetic hourly counts with daily + weekly seasonality and the project's exog columns."""
    rng = np.random.default_rng(seed)
    ds = pd.date_range("2025-01-06", periods=hours, freq="h")
    frames = []
    for i in range(n_series):
        t = np.arange(hours)
        level = 50 * (i + 1)
        mu = level * (1 + 0.5 * np.sin(2 * np.pi * t / 24) + 0.2 * np.sin(2 * np.pi * t / 168)) + 5
        frames.append(
            pd.DataFrame(
                {
                    "unique_id": f"z{i:03d}",
                    "ds": ds,
                    "y": rng.poisson(mu).astype(float),
                    "series_group": "zones",
                    "is_holiday": 0,
                    "is_holiday_adjacent": 0,
                    "fcst_temp_c": 10 + rng.normal(size=hours),
                    "fcst_precip_mm": 0.0,
                    "fcst_snowfall_cm": 0.0,
                    "fcst_wind_kmh": 10.0,
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


@pytest.fixture
def series() -> pd.DataFrame:
    return make_series()
