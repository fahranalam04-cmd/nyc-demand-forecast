"""Leakage guards: a feature row for target hour t may only use targets <= t - 24h (the origin of
a 24h-ahead forecast), and the backtest loop never hands future targets to a model."""

import itertools

import numpy as np
import pandas as pd
import pytest

from nyc_demand_forecast import backtest, models

CFG = models.LGBMConfig("test", objective="l2")


def _features(frame: pd.DataFrame) -> pd.DataFrame:
    fcst = models.make_mlforecast(CFG)
    X = fcst.preprocess(models.model_frame(frame, CFG), static_features=["zone"], dropna=False)
    return X.drop(columns=["y"]).set_index(["unique_id", "ds"]).sort_index()


def test_features_at_t_only_use_data_up_to_the_origin(series):
    t0 = series["ds"].min() + pd.Timedelta(hours=700)
    base = _features(series)
    perturbed = series.copy()
    future = perturbed["ds"] > t0
    perturbed.loc[future, "y"] = np.random.default_rng(1).integers(0, 10_000, future.sum()).astype(float)
    pert = _features(perturbed)
    ds = base.index.get_level_values("ds")
    # Target hours up to t0 + 24h have origins <= t0, so none of their features may change...
    safe = ds <= t0 + pd.Timedelta(hours=models.MIN_LAG)
    pd.testing.assert_frame_equal(base[safe], pert[safe])
    # ...while the perturbation does show up one hour later (so the test has teeth).
    later = ds == t0 + pd.Timedelta(hours=models.MIN_LAG + 1)
    assert not base[later].equals(pert[later])


def test_min_lag_covers_the_horizon():
    assert models.MIN_LAG >= 24
    assert min(models.lag_transforms()) >= 24


def test_rolling_origin_never_passes_future_targets(series):
    folds = backtest.make_folds(series["ds"].max(), n_weeks=2)
    fit_cutoffs = []

    def fit(train):
        fit_cutoffs.append(train["ds"].max())

    def predict(_model, hist, future_exog):
        assert "y" not in future_exog.columns
        assert hist["ds"].max() < future_exog["ds"].min()
        return future_exog[["unique_id", "ds"]].assign(y_hat=0.0)

    pred = backtest.rolling_origin(series, folds, fit, predict, exog_cols=["fcst_temp_c"])
    assert fit_cutoffs == [f.refit_cutoff for f in folds]
    assert len(pred) == 2 * 7 * 24 * series["unique_id"].nunique()
    assert backtest.assert_backtest_causal(pred)["violations"] == 0


def test_guard_raises_on_leaky_inputs():
    t = pd.Timestamp("2025-01-01 23:00")
    hour = pd.Timedelta(hours=1)
    with pytest.raises(AssertionError):
        backtest.assert_no_leakage(t + hour, t, t, t, t + hour)  # training data past the cutoff
    with pytest.raises(AssertionError):
        backtest.assert_no_leakage(t, t, t, t, t)  # target not after the origin


def test_causality_audit_flags_bad_rows():
    bad = pd.DataFrame(
        {
            "ds": [pd.Timestamp("2025-01-01 05:00")],
            "cutoff": [pd.Timestamp("2025-01-01 06:00")],
            "refit_cutoff": [pd.Timestamp("2025-01-01 06:00")],
        }
    )
    with pytest.raises(AssertionError):
        backtest.assert_backtest_causal(bad)


def test_seasonal_naive_is_causal_and_exact(series):
    folds = backtest.make_folds(series["ds"].max(), n_weeks=1)
    pred = backtest.run_seasonal_naive(series, folds, season=168)
    lookup = series.set_index(["unique_id", "ds"])["y"]
    expected = [lookup[(u, d - pd.Timedelta(hours=168))] for u, d in zip(pred["unique_id"], pred["ds"], strict=True)]
    np.testing.assert_allclose(pred["y_hat"], expected)
    with pytest.raises(ValueError):
        backtest.run_seasonal_naive(series, folds, season=12)


def test_folds_are_weekly_with_daily_origins(series):
    last = series["ds"].max()
    folds = backtest.make_folds(last, n_weeks=3, skip_weeks=1)
    origins = [c for f in folds for c in f.cutoffs]
    assert len(folds) == 3 and len(origins) == 21
    assert all(b - a == pd.Timedelta(hours=24) for a, b in itertools.pairwise(origins))
    assert origins[-1] + pd.Timedelta(hours=24) == last - pd.Timedelta(weeks=1)
