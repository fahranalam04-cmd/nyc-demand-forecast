"""Model specifications: global LightGBM via mlforecast, plus the statistical baselines.

Leakage by construction: every lag (and every rolling window, which is applied *on top of* a lag)
is >= the 24-hour horizon. For a forecast issued at origin T for hours T+1..T+24, the latest
target value any feature can touch is y[T+h-24] <= y[T]. mlforecast's recursive predict therefore
never feeds a prediction back in, and the "direct" and "recursive" strategies coincide.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
from mlforecast import MLForecast
from mlforecast.lag_transforms import RollingMean, RollingStd, SeasonalRollingMean

from nyc_demand_forecast import config
from nyc_demand_forecast.features import CALENDAR_COLS, WEATHER_COLS

LAGS = [24, 25, 48, 72, 168, 336]
MIN_LAG = min(LAGS)
# History needed to compute every feature at prediction time: lag 168 + 4 weekly seasons.
HISTORY_HOURS = 168 + 168 * 4 + 24


def hour_of_week(dates: pd.DatetimeIndex) -> np.ndarray:
    return (dates.dayofweek * 24 + dates.hour).to_numpy()


def lag_transforms() -> dict[int, list[Any]]:
    return {
        24: [
            RollingMean(window_size=24),
            RollingMean(window_size=168),
            RollingStd(window_size=168),
            SeasonalRollingMean(season_length=24, window_size=7),  # same hour, last 7 days
        ],
        168: [SeasonalRollingMean(season_length=168, window_size=4)],  # same hour-of-week, 4 wks
    }


@dataclass(frozen=True)
class LGBMConfig:
    name: str
    objective: str = "l2"
    use_weather: bool = True
    params: dict[str, Any] = field(default_factory=dict)

    def exog_cols(self) -> list[str]:
        return CALENDAR_COLS + (WEATHER_COLS if self.use_weather else [])

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


BASE_PARAMS: dict[str, Any] = {
    "n_estimators": 500,
    "learning_rate": 0.05,
    "num_leaves": 63,
    "min_child_samples": 50,
    "subsample": 0.8,
    "subsample_freq": 1,
    "colsample_bytree": 0.8,
    "reg_lambda": 1.0,
    "verbosity": -1,
}

# Candidates compared on the validation weeks (never on the test weeks).
CANDIDATES: list[LGBMConfig] = [
    LGBMConfig("lgbm_l2", objective="l2"),
    LGBMConfig("lgbm_l1", objective="l1"),
    LGBMConfig("lgbm_poisson", objective="poisson"),
    LGBMConfig("lgbm_tweedie", objective="tweedie", params={"tweedie_variance_power": 1.2}),
]


def make_lgbm(cfg: LGBMConfig, seed: int | None = None) -> lgb.LGBMRegressor:
    params = {**BASE_PARAMS, **cfg.params}
    return lgb.LGBMRegressor(
        objective=cfg.objective,
        random_state=config.CONFIG.seed if seed is None else seed,
        n_jobs=config.N_THREADS,
        **params,
    )


def make_mlforecast(cfg: LGBMConfig) -> MLForecast:
    return MLForecast(
        models={"lgbm": make_lgbm(cfg)},
        freq="h",
        lags=LAGS,
        lag_transforms=lag_transforms(),
        date_features=["hour", "dayofweek", "month", hour_of_week],
        num_threads=1,
    )


def model_frame(df: pd.DataFrame, cfg: LGBMConfig) -> pd.DataFrame:
    """Columns the model may see: ids, target, the zone as a categorical, and the exog features."""
    out = df[["unique_id", "ds", "y", *cfg.exog_cols()]].copy()
    out["zone"] = out["unique_id"].astype("category")
    return out
