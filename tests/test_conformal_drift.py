import numpy as np
import pandas as pd
import pytest

from nyc_demand_forecast import conformal, drift


def _backtest_frame(days: int = 60, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    for d in range(days):
        cutoff = pd.Timestamp("2025-01-01 23:00") + pd.Timedelta(days=d)
        ds = cutoff + pd.to_timedelta(np.arange(1, 25), unit="h")
        yhat = 50 + 30 * np.sin(np.arange(24) / 24 * 2 * np.pi)
        y = yhat + rng.normal(0, np.sqrt(yhat + 1))
        rows.append(pd.DataFrame({"unique_id": "a", "ds": ds, "cutoff": cutoff, "y_hat": yhat, "y": y}))
    return pd.concat(rows, ignore_index=True)


def test_finite_sample_conformal_quantile():
    scores = np.arange(1, 11, dtype=float)  # n = 10
    assert conformal.conformal_quantile(scores, alpha=0.2) == 9.0  # ceil(11 * 0.8) = 9th smallest
    assert conformal.conformal_quantile(scores[:3], alpha=0.05) == float("inf")


def test_coverage_is_near_nominal_for_exchangeable_errors():
    out = conformal.add_intervals(_backtest_frame(), levels=(80, 95), calib_days=28)
    rep = conformal.coverage_report(out[out["n_calib"] >= 28 * 24], (80, 95))
    assert rep["80"]["empirical_coverage"] == pytest.approx(0.80, abs=0.04)
    assert rep["95"]["empirical_coverage"] == pytest.approx(0.95, abs=0.03)


def test_intervals_never_use_residuals_from_after_the_origin():
    frame = _backtest_frame()
    base = conformal.add_intervals(frame, calib_days=28)
    cut = frame["cutoff"].unique()[40]
    shocked = frame.copy()
    shocked.loc[shocked["ds"] > cut, "y"] += 1e6  # wreck every residual after this origin
    after = conformal.add_intervals(shocked, calib_days=28)
    keep = base["cutoff"] <= cut
    pd.testing.assert_series_equal(base.loc[keep, "hi_95"], after.loc[keep, "hi_95"])


def test_psi_near_zero_for_same_distribution_and_large_for_shift():
    rng = np.random.default_rng(0)
    ref = rng.normal(size=20_000)
    assert drift.psi(ref, rng.normal(size=20_000)) < 0.01
    shifted = drift.psi(ref, rng.normal(1.0, 1.0, size=20_000))
    assert shifted > drift.SIGNIFICANT and drift.level(shifted) == "significant"


def test_psi_handles_discrete_features_and_matches_formula():
    hours = np.repeat(np.arange(24), 100)
    assert drift.psi(hours, hours.copy()) == pytest.approx(0.0, abs=1e-9)
    expected = (0.8 - 0.5) * np.log(0.8 / 0.5) + (0.2 - 0.5) * np.log(0.2 / 0.5)
    assert drift.psi_from_counts(np.array([50, 50]), np.array([80, 20])) == pytest.approx(expected)
