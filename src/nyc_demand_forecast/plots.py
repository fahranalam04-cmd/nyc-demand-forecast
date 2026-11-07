"""The three README figures (static PNG, small). Colours follow the model identity in a fixed
order and every chart has a single y-axis."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT_2 = "#52514e"
GRID = "#e4e3df"
NEUTRAL = "#8a8984"
# Fixed categorical order: identity never depends on rank.
MODEL_COLORS = {
    "lgbm": "#2a78d6",
    "seasonal_naive_168h": "#eb6834",
    "mstl": "#1baf7a",
    "seasonal_naive_24h": "#eda100",
    "lgbm_frozen": "#6250d6",
}
MODEL_LABELS = {
    "lgbm": "LightGBM (weekly refit)",
    "lgbm_frozen": "LightGBM (frozen at test start)",
    "seasonal_naive_168h": "Seasonal naive (same hour last week)",
    "seasonal_naive_24h": "Seasonal naive (same hour yesterday)",
    "mstl": "MSTL + AutoETS",
}


def _style(ax: plt.Axes) -> None:
    ax.set_facecolor(SURFACE)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=TEXT_2, labelsize=9)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def _fig(width: float, height: float, nrows: int = 1):  # noqa: ANN202
    fig, axes = plt.subplots(nrows, 1, figsize=(width, height), dpi=110, facecolor=SURFACE)
    return fig, axes


def forecast_vs_actual(pred: pd.DataFrame, unique_id: str, title: str, path: Path, days: int = 7) -> None:
    """Last ``days`` test days for one series: actual, LightGBM forecast, 80/95% intervals."""
    g = pred[(pred["unique_id"] == unique_id) & (pred["model"] == "lgbm")].sort_values("ds")
    g = g[g["ds"] > g["ds"].max() - pd.Timedelta(days=days)]
    sn = pred[(pred["unique_id"] == unique_id) & (pred["model"] == "seasonal_naive_168h")]
    sn = sn[sn["ds"].isin(g["ds"])].sort_values("ds")
    fig, ax = _fig(9, 3.4)
    _style(ax)
    blue = MODEL_COLORS["lgbm"]
    ax.fill_between(g["ds"], g["lo_95"], g["hi_95"], color=blue, alpha=0.12, linewidth=0, label="95% interval")
    ax.fill_between(g["ds"], g["lo_80"], g["hi_80"], color=blue, alpha=0.22, linewidth=0, label="80% interval")
    ax.plot(sn["ds"], sn["y_hat"], color=MODEL_COLORS["seasonal_naive_168h"], linewidth=1.2,
            linestyle=(0, (3, 2)), label=MODEL_LABELS["seasonal_naive_168h"])
    ax.plot(g["ds"], g["y_hat"], color=blue, linewidth=2, label=MODEL_LABELS["lgbm"])
    ax.plot(g["ds"], g["y"], color=TEXT, linewidth=1.4, label="Actual pickups")
    ax.set_ylabel("Pickups per hour", color=TEXT_2, fontsize=9)
    ax.set_title(title, loc="left", color=TEXT, fontsize=11)
    ax.legend(frameon=False, fontsize=8, ncol=3, loc="upper left", bbox_to_anchor=(0, -0.12))
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def wape_by_model(summary: pd.DataFrame, title: str, path: Path) -> None:
    """Horizontal bars of test WAPE (%), best at top, value labels at bar ends."""
    s = summary.sort_values("wape", ascending=True)
    fig, ax = _fig(7.5, 0.55 * len(s) + 1.0)
    _style(ax)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.grid(axis="y", visible=False)
    labels = [MODEL_LABELS.get(m, m) for m in s["model"]]
    colors = [MODEL_COLORS.get(m, NEUTRAL) for m in s["model"]]
    y = range(len(s))
    ax.barh(list(y), s["wape"] * 100, color=colors, height=0.55)
    for yi, v in zip(y, s["wape"] * 100, strict=True):
        ax.text(v + 0.3, yi, f"{v:.1f}%", va="center", fontsize=9, color=TEXT)
    ax.set_yticks(list(y), labels, fontsize=9, color=TEXT)
    ax.invert_yaxis()
    ax.set_xlabel("WAPE, % (lower is better)", color=TEXT_2, fontsize=9)
    ax.set_title(title, loc="left", color=TEXT, fontsize=11)
    ax.set_xlim(0, float(s["wape"].max() * 100) * 1.15)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)


def error_over_time_and_drift(weekly: pd.DataFrame, monthly_psi: pd.DataFrame, path: Path,
                              models: list[str]) -> None:
    """Top: weekly test WAPE per model. Bottom: monthly PSI of trip attributes vs baseline."""
    fig, (ax1, ax2) = _fig(9, 6.2, nrows=2)
    for ax in (ax1, ax2):
        _style(ax)
    for m in models:
        w = weekly[weekly["model"] == m].sort_values("week_start")
        ax1.plot(w["week_start"], w["wape"] * 100, color=MODEL_COLORS.get(m, NEUTRAL), linewidth=2,
                 marker="o", markersize=4, label=MODEL_LABELS.get(m, m))
    ax1.set_ylabel("Weekly WAPE, %", color=TEXT_2, fontsize=9)
    ax1.set_title("Backtest error by test week (top-50 zones)", loc="left", color=TEXT, fontsize=11)
    ax1.legend(frameon=False, fontsize=8, ncol=2, loc="upper left")
    ax1.set_ylim(bottom=0)

    attr_colors = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
    for color, (attr, g) in zip(attr_colors, monthly_psi.groupby("attribute", sort=True), strict=False):
        g = g.sort_values("source_month")
        ax2.plot(pd.to_datetime(g["source_month"]), g["psi"], color=color, linewidth=2, label=attr)
    ax2.axhline(0.10, color=NEUTRAL, linewidth=1, linestyle=(0, (4, 3)))
    ax2.axhline(0.25, color=TEXT_2, linewidth=1, linestyle=(0, (4, 3)))
    ax2.text(ax2.get_xlim()[0], 0.255, " significant (0.25)", fontsize=8, color=TEXT_2, va="bottom")
    ax2.text(ax2.get_xlim()[0], 0.105, " moderate (0.10)", fontsize=8, color=NEUTRAL, va="bottom")
    ax2.set_ylabel("PSI vs Oct-Dec 2024", color=TEXT_2, fontsize=9)
    ax2.set_title("Monthly drift of clean-trip attributes", loc="left", color=TEXT, fontsize=11)
    ax2.legend(frameon=False, fontsize=8, ncol=4, loc="upper left")
    ax2.set_ylim(bottom=0)
    fig.tight_layout()
    fig.savefig(path, facecolor=SURFACE)
    plt.close(fig)
