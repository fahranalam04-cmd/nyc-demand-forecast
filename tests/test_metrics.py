import numpy as np
import pandas as pd
import pytest

from nyc_demand_forecast import metrics


def test_wape_is_volume_weighted():
    y, yhat = np.array([100.0, 0.0, 10.0]), np.array([90.0, 5.0, 10.0])
    assert metrics.wape(y, yhat) == pytest.approx(15 / 110)


def test_smape_defines_zero_over_zero_as_zero():
    assert metrics.smape(np.array([0.0, 0.0]), np.array([0.0, 0.0])) == 0.0
    assert metrics.smape(np.array([0.0]), np.array([4.0])) == pytest.approx(2.0)


def test_seasonal_naive_scale_uses_lag_168_mae():
    ds = pd.date_range("2025-01-01", periods=168 * 3, freq="h")
    y = np.concatenate([np.full(168, 10.0), np.full(168, 14.0), np.full(168, 10.0)])
    scale = metrics.seasonal_naive_scale(pd.DataFrame({"unique_id": "a", "ds": ds, "y": y}))
    assert scale["a"] == pytest.approx(4.0)


def test_mase_equals_one_when_error_matches_the_scale():
    assert metrics.mase(np.array([1.0, 3.0]), np.array([3.0, 1.0]), scale=2.0) == pytest.approx(1.0)


def test_summarize_pools_wape_and_averages_mase_over_series():
    pred = pd.DataFrame(
        {
            "model": "m",
            "unique_id": ["a", "a", "b", "b"],
            "y": [10.0, 10.0, 100.0, 100.0],
            "y_hat": [12.0, 8.0, 100.0, 100.0],
        }
    )
    out = metrics.summarize(pred, pd.Series({"a": 1.0, "b": 1.0})).iloc[0]
    assert out["wape"] == pytest.approx(4 / 220)
    assert out["mase"] == pytest.approx((2.0 + 0.0) / 2)
