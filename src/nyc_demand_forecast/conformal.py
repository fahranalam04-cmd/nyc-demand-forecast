"""Online, normalised split-conformal prediction intervals for rolling-origin forecasts.

For each series and each forecast origin T, the calibration set is the out-of-sample forecasts
*whose target hour is <= T* within a trailing window (default 28 days). Those forecasts were all
issued at earlier origins, so the interval at T uses no information from after T.

Count noise grows with the level, so the conformity score is normalised:
    s = |y - yhat| / sqrt(max(yhat, 0) + 1)
and the interval is yhat +/- q * sqrt(max(yhat, 0) + 1), clipped at 0, where q is the
finite-sample-corrected ceil((n + 1)(1 - alpha))-th smallest calibration score.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd


def _sigma(yhat: np.ndarray) -> np.ndarray:
    return np.sqrt(np.clip(yhat, 0, None) + 1.0)


def conformity_scores(y: np.ndarray, yhat: np.ndarray) -> np.ndarray:
    return np.abs(np.asarray(y, float) - np.asarray(yhat, float)) / _sigma(np.asarray(yhat, float))


def conformal_quantile(scores: np.ndarray, alpha: float) -> float:
    """Finite-sample split-conformal quantile; +inf when there are too few scores."""
    scores = np.sort(np.asarray(scores, float))
    n = scores.size
    k = math.ceil((n + 1) * (1 - alpha))
    if n == 0 or k > n:
        return float("inf")
    return float(scores[k - 1])


def add_intervals(
    pred: pd.DataFrame,
    levels: tuple[int, ...] = (80, 95),
    calib_days: int = 28,
) -> pd.DataFrame:
    """Add ``lo_<lvl>``/``hi_<lvl>`` columns to a backtest frame with unique_id, ds, cutoff, y, y_hat.

    Each row's interval uses only rows of the same series with ``ds <= cutoff`` (earlier origins).
    """
    out = []
    window = pd.Timedelta(days=calib_days)
    for _uid, g in pred.sort_values(["unique_id", "cutoff", "ds"]).groupby("unique_id", sort=False):
        g = g.copy()
        scores = conformity_scores(g["y"].to_numpy(), g["y_hat"].to_numpy())
        ds = g["ds"].to_numpy()
        sig = _sigma(g["y_hat"].to_numpy())
        for lvl in levels:
            g[f"lo_{lvl}"] = np.nan
            g[f"hi_{lvl}"] = np.nan
        g["n_calib"] = 0
        for cutoff, idx in g.groupby("cutoff").indices.items():
            mask = (ds <= np.datetime64(cutoff)) & (ds > np.datetime64(cutoff - window))
            calib = scores[mask]
            g.iloc[idx, g.columns.get_loc("n_calib")] = calib.size
            for lvl in levels:
                q = conformal_quantile(calib, alpha=1 - lvl / 100)
                lo = np.clip(g["y_hat"].to_numpy()[idx] - q * sig[idx], 0, None)
                hi = g["y_hat"].to_numpy()[idx] + q * sig[idx]
                g.iloc[idx, g.columns.get_loc(f"lo_{lvl}")] = lo
                g.iloc[idx, g.columns.get_loc(f"hi_{lvl}")] = hi
        out.append(g)
    return pd.concat(out, ignore_index=True)


def coverage_report(pred: pd.DataFrame, levels: tuple[int, ...] = (80, 95)) -> dict[str, dict[str, float]]:
    report = {}
    for lvl in levels:
        lo, hi = pred[f"lo_{lvl}"], pred[f"hi_{lvl}"]
        inside = (pred["y"] >= lo) & (pred["y"] <= hi)
        width = (hi - lo) / pred["y"].clip(lower=1)
        report[str(lvl)] = {
            "nominal": lvl / 100,
            "empirical_coverage": float(inside.mean()),
            "median_relative_width": float(width.median()),
            "n": len(pred),
        }
    return report
