"""Population Stability Index (PSI) drift monitoring.

PSI = sum_b (cur_b - ref_b) * ln(cur_b / ref_b) over bins defined by the *reference* deciles.
Conventional reading: < 0.10 stable, 0.10-0.25 moderate shift, > 0.25 significant shift.

Two monitors:
* model features - the exact feature matrix the model consumes (via mlforecast.preprocess),
  training window vs the latest month;
* raw trip attributes - distance / fare / total / duration per month vs a fixed baseline quarter,
  binned and counted inside DuckDB so the ~90M trips never leave the warehouse.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

EPS = 1e-4
MODERATE, SIGNIFICANT = 0.10, 0.25


def bin_edges(reference: np.ndarray, n_bins: int = 10) -> np.ndarray:
    ref = np.asarray(reference, float)
    ref = ref[np.isfinite(ref)]
    edges = np.unique(np.quantile(ref, np.linspace(0, 1, n_bins + 1)[1:-1]))
    return edges


def psi_from_counts(ref_counts: np.ndarray, cur_counts: np.ndarray, eps: float = EPS) -> float:
    ref = np.asarray(ref_counts, float)
    cur = np.asarray(cur_counts, float)
    ref_p = np.clip(ref / ref.sum(), eps, None)
    cur_p = np.clip(cur / cur.sum(), eps, None)
    return float(np.sum((cur_p - ref_p) * np.log(cur_p / ref_p)))


def psi(reference: np.ndarray, current: np.ndarray, n_bins: int = 10) -> float:
    """PSI of ``current`` against ``reference`` using reference-quantile bins.

    Discrete features with few distinct values get one bin per value automatically (np.unique on
    the quantile edges collapses duplicates)."""
    edges = bin_edges(reference, n_bins)
    ref = np.asarray(reference, float)
    cur = np.asarray(current, float)
    ref_counts = np.bincount(np.searchsorted(edges, ref[np.isfinite(ref)], side="left"),
                             minlength=edges.size + 1)
    cur_counts = np.bincount(np.searchsorted(edges, cur[np.isfinite(cur)], side="left"),
                             minlength=edges.size + 1)
    return psi_from_counts(ref_counts, cur_counts)


def level(value: float) -> str:
    if value >= SIGNIFICANT:
        return "significant"
    if value >= MODERATE:
        return "moderate"
    return "stable"


def feature_psi(ref: pd.DataFrame, cur: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    rows = [{"feature": c, "psi": psi(ref[c].to_numpy(), cur[c].to_numpy())} for c in columns]
    out = pd.DataFrame(rows)
    out["level"] = out["psi"].map(level)
    return out.sort_values("psi", ascending=False).reset_index(drop=True)


TRIP_ATTRIBUTES = ["trip_distance_mi", "fare_amount", "total_amount", "duration_min"]


def trip_attribute_psi_by_month(con, baseline_months: tuple[str, str], columns: list[str] | None = None,  # noqa: ANN001
                                n_bins: int = 10) -> pd.DataFrame:
    """Monthly PSI of clean-trip attributes vs a baseline window, computed in DuckDB.

    ``baseline_months`` = (first, last) inclusive 'YYYY-MM-01' dates.
    """
    columns = columns or TRIP_ATTRIBUTES
    lo, hi = baseline_months
    out = []
    for col in columns:
        qs = ", ".join(f"{q:.2f}" for q in np.linspace(0, 1, n_bins + 1)[1:-1])
        edges = con.execute(
            f"select quantile_cont({col}, [{qs}]) from intermediate.int_trips__clean "
            f"where source_month between date '{lo}' and date '{hi}' and {col} is not null"
        ).fetchone()[0]
        edges = sorted(set(float(e) for e in edges))
        case = "case " + " ".join(f"when {col} <= {e!r} then {i}" for i, e in enumerate(edges))
        case += f" else {len(edges)} end"
        counts = con.execute(
            f"select source_month, {case} as b, count(*) as n from intermediate.int_trips__clean "
            f"where {col} is not null group by 1, 2"
        ).df()
        wide = counts.pivot_table(index="source_month", columns="b", values="n", fill_value=0)
        base = wide.loc[(wide.index >= pd.Timestamp(lo)) & (wide.index <= pd.Timestamp(hi))].sum()
        for month, row in wide.iterrows():
            out.append({"source_month": pd.Timestamp(month).strftime("%Y-%m"), "attribute": col,
                        "psi": psi_from_counts(base.to_numpy(), row.to_numpy())})
    df = pd.DataFrame(out)
    df["level"] = df["psi"].map(level)
    return df
