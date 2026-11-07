"""Point-forecast metrics. Conventions are explicit because hourly counts contain zeros.

* WAPE  = sum|y - yhat| / sum|y|                      (pooled; volume-weighted)
* MASE  = MAE / MAE of the in-sample seasonal-naive (m=168) on the *training* history, per series;
          the overall figure is the mean over series.
* sMAPE = mean(2|y - yhat| / (|y| + |yhat|)), with 0/0 := 0.
All three are returned as fractions (multiply by 100 for percent where noted in reports).
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def wape(y: np.ndarray, yhat: np.ndarray) -> float:
    y, yhat = np.asarray(y, float), np.asarray(yhat, float)
    denom = np.abs(y).sum()
    return float(np.abs(y - yhat).sum() / denom) if denom > 0 else float("nan")


def smape(y: np.ndarray, yhat: np.ndarray) -> float:
    y, yhat = np.asarray(y, float), np.asarray(yhat, float)
    denom = np.abs(y) + np.abs(yhat)
    ratio = np.divide(2 * np.abs(y - yhat), denom, out=np.zeros_like(denom), where=denom > 0)
    return float(ratio.mean())


def seasonal_naive_scale(train: pd.DataFrame, season: int = 168) -> pd.Series:
    """Per-series in-sample MAE of the seasonal-naive forecast y[t-season] (the MASE denominator).

    ``train`` must be a regular hourly frame (unique_id, ds, y) restricted to training data only.
    """
    train = train.sort_values(["unique_id", "ds"])
    diffs = train.groupby("unique_id")["y"].diff(season).abs()
    return diffs.groupby(train["unique_id"]).mean()


def mase(y: np.ndarray, yhat: np.ndarray, scale: float) -> float:
    mae = float(np.mean(np.abs(np.asarray(y, float) - np.asarray(yhat, float))))
    return mae / scale if scale > 0 else float("nan")


def summarize(pred: pd.DataFrame, scale: pd.Series, model_col: str = "model") -> pd.DataFrame:
    """Overall metrics per model: pooled WAPE, mean-over-series MASE, pointwise sMAPE."""
    rows = []
    for model, g in pred.groupby(model_col):
        per_series = per_series_metrics(g, scale)
        rows.append(
            {
                "model": model,
                "wape": wape(g["y"], g["y_hat"]),
                "mase": float(per_series["mase"].mean()),
                "smape": smape(g["y"], g["y_hat"]),
                "n_points": int(len(g)),
                "n_series": int(g["unique_id"].nunique()),
            }
        )
    return pd.DataFrame(rows).sort_values("wape").reset_index(drop=True)


def per_series_metrics(pred: pd.DataFrame, scale: pd.Series) -> pd.DataFrame:
    rows = []
    for uid, g in pred.groupby("unique_id"):
        rows.append(
            {
                "unique_id": uid,
                "wape": wape(g["y"], g["y_hat"]),
                "mase": mase(g["y"], g["y_hat"], float(scale.loc[uid])),
                "smape": smape(g["y"], g["y_hat"]),
                "mean_y": float(g["y"].mean()),
            }
        )
    return pd.DataFrame(rows)
