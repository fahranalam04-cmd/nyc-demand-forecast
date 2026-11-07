"""Rolling-origin (expanding-window) backtest for day-ahead hourly forecasts.

Protocol
--------
* An *origin* (cutoff) is 23:00 of day D; the forecast covers the 24 hours of day D+1.
* Origins are daily; models are re-fitted weekly on all data <= the first origin of the week.
  One week of daily origins = one *fold*.
* Timeline (most recent last):  ... | validation (model selection) | calibration (conformal
  warm-up) | test | end. Test weeks are scored once, with the configuration chosen on validation.

Every fold asserts its own leakage guard (train <= refit cutoff, history <= origin, forecast
targets strictly after the origin) so a refactor that breaks causality fails loudly.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass

import pandas as pd

from nyc_demand_forecast import models

log = logging.getLogger(__name__)
HOUR = pd.Timedelta(hours=1)


@dataclass(frozen=True)
class Fold:
    fold: int
    refit_cutoff: pd.Timestamp
    cutoffs: tuple[pd.Timestamp, ...]
    phase: str


def make_folds(last_ds: pd.Timestamp, n_weeks: int, skip_weeks: int = 0, phase: str = "test",
               horizon: int = 24) -> list[Fold]:
    """Weekly folds of daily origins ending ``skip_weeks`` weeks before ``last_ds``.

    The last origin of the last fold is ``end - horizon`` so its forecast ends exactly at ``end``.
    """
    end = pd.Timestamp(last_ds) - pd.Timedelta(weeks=skip_weeks)
    first = end - pd.Timedelta(weeks=n_weeks)
    folds = []
    for w in range(n_weeks):
        week_start = first + pd.Timedelta(weeks=w)
        cutoffs = tuple(week_start + pd.Timedelta(hours=horizon * d) for d in range(7))
        folds.append(Fold(fold=w, refit_cutoff=cutoffs[0], cutoffs=cutoffs, phase=phase))
    return folds


def assert_no_leakage(train_max: pd.Timestamp, refit_cutoff: pd.Timestamp, hist_max: pd.Timestamp,
                      cutoff: pd.Timestamp, target_min: pd.Timestamp) -> None:
    if train_max > refit_cutoff:
        raise AssertionError(f"training data ({train_max}) extends past refit cutoff {refit_cutoff}")
    if hist_max > cutoff:
        raise AssertionError(f"history ({hist_max}) extends past origin {cutoff}")
    if target_min <= cutoff:
        raise AssertionError(f"forecast target {target_min} is not after origin {cutoff}")


FitFn = Callable[[pd.DataFrame], object]
PredictFn = Callable[[object, pd.DataFrame, pd.DataFrame], pd.DataFrame]


def rolling_origin(
    frame: pd.DataFrame,
    folds: Iterable[Fold],
    fit: FitFn,
    predict: PredictFn,
    exog_cols: list[str],
    horizon: int = 24,
    history_hours: int = models.HISTORY_HOURS,
    refit: bool = True,
) -> pd.DataFrame:
    """Generic loop. ``fit(train) -> model``; ``predict(model, history, future_exog) -> frame``
    with columns unique_id, ds, y_hat. Future target values are never passed to ``predict``."""
    out = []
    model = None
    for fold in folds:
        if refit or model is None:
            train = frame[frame["ds"] <= fold.refit_cutoff]
            t0 = time.perf_counter()
            model = fit(train)
            train_max = train["ds"].max()
            log.info("fold %s/%d fit on %d rows <= %s in %.1fs", fold.phase, fold.fold, len(train),
                     fold.refit_cutoff, time.perf_counter() - t0)
        for cutoff in fold.cutoffs:
            hist = frame[(frame["ds"] <= cutoff) & (frame["ds"] > cutoff - history_hours * HOUR)]
            fut = frame[(frame["ds"] > cutoff) & (frame["ds"] <= cutoff + horizon * HOUR)]
            assert_no_leakage(train_max, fold.refit_cutoff if refit else train_max,
                              hist["ds"].max(), cutoff, fut["ds"].min())
            pred = predict(model, hist, fut[["unique_id", "ds", *exog_cols]])
            pred = pred.merge(fut[["unique_id", "ds", "y"]], on=["unique_id", "ds"], how="inner")
            if len(pred) != len(fut):
                raise ValueError(f"prediction rows {len(pred)} != target rows {len(fut)} at {cutoff}")
            pred["cutoff"] = cutoff
            pred["fold"] = fold.fold
            pred["phase"] = fold.phase
            pred["refit_cutoff"] = fold.refit_cutoff if refit else train_max
            out.append(pred)
    return pd.concat(out, ignore_index=True)


# --------------------------------------------------------------------------------------------
# Concrete models
# --------------------------------------------------------------------------------------------

def lgbm_fit_predict(cfg: models.LGBMConfig) -> tuple[FitFn, PredictFn]:
    def fit(train: pd.DataFrame):  # noqa: ANN202
        fcst = models.make_mlforecast(cfg)
        fcst.fit(train, static_features=["zone"])
        return fcst

    def predict(fcst, hist: pd.DataFrame, future_exog: pd.DataFrame) -> pd.DataFrame:  # noqa: ANN001
        p = fcst.predict(h=len(future_exog["ds"].unique()), new_df=hist, X_df=future_exog)
        return p.rename(columns={"lgbm": "y_hat"})[["unique_id", "ds", "y_hat"]]

    return fit, predict


def run_lgbm(frame: pd.DataFrame, cfg: models.LGBMConfig, folds: list[Fold], refit: bool = True) -> pd.DataFrame:
    fit, predict = lgbm_fit_predict(cfg)
    mf = models.model_frame(frame, cfg)
    pred = rolling_origin(mf, folds, fit, predict, exog_cols=cfg.exog_cols(), refit=refit)
    pred["y_hat"] = pred["y_hat"].clip(lower=0)
    pred["model"] = cfg.name if refit else f"{cfg.name}_frozen"
    return pred


def run_seasonal_naive(frame: pd.DataFrame, folds: list[Fold], season: int) -> pd.DataFrame:
    """y_hat(t) = y(t - season). For t in (T, T+24] and season >= 24, t - season <= T."""
    if season < 24:
        raise ValueError("seasonal naive season must be >= the 24h horizon to stay causal")
    shifted = frame[["unique_id", "ds", "y"]].copy()
    shifted["ds"] = shifted["ds"] + season * HOUR
    shifted = shifted.rename(columns={"y": "y_hat"})

    def fit(train: pd.DataFrame) -> None:
        return None

    def predict(_model, hist: pd.DataFrame, future_exog: pd.DataFrame) -> pd.DataFrame:  # noqa: ANN001
        p = future_exog[["unique_id", "ds"]].merge(shifted, on=["unique_id", "ds"], how="left")
        if p["y_hat"].isna().any():
            raise ValueError("seasonal naive needs at least `season` hours of history")
        return p

    pred = rolling_origin(frame, folds, fit, predict, exog_cols=[], history_hours=season)
    pred["model"] = f"seasonal_naive_{season}h"
    return pred


def run_mstl(frame: pd.DataFrame, folds: list[Fold], input_weeks: int = 8, n_jobs: int = 4) -> pd.DataFrame:
    """MSTL (daily + weekly seasonality, AutoETS trend) refit at every daily origin on the last
    ``input_weeks`` weeks, via statsforecast's cross-validation over the same origins."""
    from statsforecast import StatsForecast
    from statsforecast.models import MSTL, AutoETS

    cutoffs = [c for f in folds for c in f.cutoffs]
    end = cutoffs[-1] + 24 * HOUR
    data = frame.loc[frame["ds"] <= end, ["unique_id", "ds", "y"]]
    sf = StatsForecast(
        models=[MSTL(season_length=[24, 168], trend_forecaster=AutoETS(model="ZZN"), alias="mstl")],
        freq="h",
        n_jobs=n_jobs,
    )
    cv = sf.cross_validation(df=data, h=24, step_size=24, n_windows=len(cutoffs),
                             input_size=input_weeks * 168)
    cv = cv.rename(columns={"mstl": "y_hat"})
    got = sorted(pd.to_datetime(cv["cutoff"].unique()))
    if [pd.Timestamp(c) for c in got] != cutoffs:
        raise AssertionError("statsforecast cutoffs do not match the backtest origins")
    fold_of = {c: (f.fold, f.phase, f.refit_cutoff) for f in folds for c in f.cutoffs}
    cv["fold"] = cv["cutoff"].map(lambda c: fold_of[pd.Timestamp(c)][0])
    cv["phase"] = cv["cutoff"].map(lambda c: fold_of[pd.Timestamp(c)][1])
    cv["refit_cutoff"] = cv["cutoff"]
    cv["y_hat"] = cv["y_hat"].clip(lower=0)
    cv["model"] = "mstl"
    return cv[["unique_id", "ds", "y_hat", "y", "cutoff", "fold", "phase", "refit_cutoff", "model"]]


def horizon_step(pred: pd.DataFrame) -> pd.Series:
    return ((pred["ds"] - pred["cutoff"]) / HOUR).astype(int)


def weekly_wape(pred: pd.DataFrame) -> pd.DataFrame:
    g = pred.assign(abs_err=(pred["y"] - pred["y_hat"]).abs())
    agg = g.groupby(["model", "fold"]).agg(abs_err=("abs_err", "sum"), y=("y", "sum"),
                                           week_start=("ds", "min"))
    agg["wape"] = agg["abs_err"] / agg["y"]
    return agg.reset_index()[["model", "fold", "week_start", "wape"]]


def assert_backtest_causal(pred: pd.DataFrame) -> dict[str, int]:
    """Post-hoc audit of a backtest frame: every target after its origin, every refit not after
    its origin, horizon within 1..24. Returns counts for reporting."""
    steps = horizon_step(pred)
    bad_target = int((pred["ds"] <= pred["cutoff"]).sum())
    bad_refit = int((pred["refit_cutoff"] > pred["cutoff"]).sum())
    bad_step = int(((steps < 1) | (steps > 24)).sum())
    if bad_target or bad_refit or bad_step:
        raise AssertionError(f"causality violated: target={bad_target} refit={bad_refit} step={bad_step}")
    return {"rows_checked": int(len(pred)), "violations": 0,
            "max_horizon_h": int(steps.max()), "min_horizon_h": int(steps.min())}
