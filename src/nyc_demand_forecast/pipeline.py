"""Pipeline steps shared by the Dagster assets and the CLI. Each step reads its inputs from the
data drive, writes outputs back there, and saves its metrics to ``reports/*.json``."""

from __future__ import annotations

import json
import logging
import pickle
import time
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd

from nyc_demand_forecast import backtest, config, conformal, drift, features, metrics, models, plots, tracking
from nyc_demand_forecast.ingest import tlc, warehouse, weather

log = logging.getLogger(__name__)
SERIES_PATH = config.FEATURES_DIR / "series.parquet"
PRED_PATH = config.FEATURES_DIR / "backtest_predictions.parquet"
GROUPS = ("zones", "citywide")


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def write_report(name: str, payload: dict[str, Any]) -> None:
    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = config.REPORTS_DIR / name
    path.write_text(json.dumps(payload, indent=2, default=_json_default) + "\n")


def _json_default(o: Any) -> Any:
    if isinstance(o, np.integer | np.floating):
        return o.item()
    if isinstance(o, pd.Timestamp | datetime):
        return o.isoformat()
    raise TypeError(type(o))


# --------------------------------------------------------------------------------------------
# Ingestion
# --------------------------------------------------------------------------------------------

def ingest_trips() -> dict[str, Any]:
    months = tlc.month_range(config.CONFIG.start_month, config.CONFIG.end_month)
    manifest = tlc.ingest_tlc(months)
    con = config.duckdb_connect()
    try:
        n_view = warehouse.register_trips(con)
    finally:
        con.close()
    per_month = {m: {k: manifest[m][k] for k in ("rows", "bytes", "sha256", "last_action")} for m in months}
    report = {
        "generated_at": _now(),
        "source": "NYC TLC Trip Record Data (yellow taxi), " + config.TLC_BASE_URL,
        "months": [months[0], months[-1]],
        "n_files": len(months),
        "rows_total": int(sum(v["rows"] for v in per_month.values())),
        "bytes_total": int(sum(v["bytes"] for v in per_month.values())),
        "rows_in_duckdb_view": int(n_view),
        "integrity": "Content-Length match + Parquet footer parse; multipart S3 ETags cannot be "
                     "recomputed locally (etag_verified=null); SHA-256 recorded for idempotent reruns",
        "schema_changes": tlc.schema_changes(manifest, months),
        "per_month": per_month,
    }
    write_report("data_ingestion_trips.json", report)
    return report


def ingest_zones() -> dict[str, Any]:
    entry = tlc.ingest_zone_lookup()
    con = config.duckdb_connect()
    try:
        n = warehouse.register_zones(con)
    finally:
        con.close()
    return {"rows": int(n), "etag_verified": entry.get("etag_verified"), "last_action": entry["last_action"]}


def ingest_weather() -> dict[str, Any]:
    manifest = weather.ingest_weather()
    con = config.duckdb_connect()
    try:
        counts = warehouse.register_weather(con)
    finally:
        con.close()
    report = {"generated_at": _now(), "products": manifest, "duckdb_rows": counts}
    write_report("data_ingestion_weather.json", report)
    return report


# --------------------------------------------------------------------------------------------
# Data-quality summary from the dbt marts
# --------------------------------------------------------------------------------------------

def data_quality_report(dbt_results: dict[str, Any] | None = None) -> dict[str, Any]:
    con = config.duckdb_connect(read_only=True)
    try:
        q = con.execute("select * from marts.rpt_trip_quality order by source_month").df()
        v = con.execute("select * from marts.rpt_vendor_anomalies").df()
        zones = con.execute("select count(*), sum(clean_pickups) from marts.dim_zones").fetchone()
        top50 = con.execute(
            "select sum(clean_pickups) filter (where volume_rank <= 50) / sum(clean_pickups) from marts.dim_zones"
        ).fetchone()[0]
        fct_rows = con.execute("select count(*) from marts.fct_hourly_zone_demand").fetchone()[0]
    finally:
        con.close()
    rule_cols = [c for c in q.columns if c.startswith("removed_")]
    totals = {c: int(q[c].sum()) for c in ["rows_raw", *rule_cols, "rows_clean"]}
    totals["rows_removed"] = totals["rows_raw"] - totals["rows_clean"]
    totals["pct_removed"] = 100 * totals["rows_removed"] / totals["rows_raw"]
    q["pct_removed"] = 100 * (1 - q["rows_clean"] / q["rows_raw"])
    vendor7 = v[v["vendor_id"] == 7]
    flex = v[(v["vendor_id"] == 2)]
    # Real pickups a naive "duration <= 0 or fare < 0" filter would ALSO have removed.
    naive = v.groupby("source_month")[["zero_duration_with_distance", "negative_fare_positive_total",
                                       "rows_raw"]].sum()
    naive_rows = naive["zero_duration_with_distance"] + naive["negative_fare_positive_total"]
    naive_pct = 100 * naive_rows / naive["rows_raw"]
    naive_pct.index = naive_pct.index.astype(str)
    report = {
        "generated_at": _now(),
        "rule_order": [c.replace("removed_", "") for c in rule_cols],
        "totals": totals,
        "max_monthly_pct_removed": float(q["pct_removed"].max()),
        "per_month": q.assign(source_month=q["source_month"].astype(str)).to_dict(orient="records"),
        "vendor_anomalies": {
            "vendor7_rows": int(vendor7["rows_raw"].sum()),
            "vendor7_zero_duration_rows": int(vendor7["zero_duration"].sum()),
            "vendor7_zero_duration_with_distance_rows": int(vendor7["zero_duration_with_distance"].sum()),
            "vendor7_first_month": str(vendor7["source_month"].min()) if len(vendor7) else None,
            "vendor2_negative_fare_positive_total_rows": int(flex["negative_fare_positive_total"].sum()),
            "kept_by_refined_rules_note": "naive 'duration<=0 or fare<0' filters would drop these real pickups",
            "naive_filter_extra_removed_pct_by_month": naive_pct.round(3).to_dict(),
            "naive_filter_extra_removed_pct_max": float(naive_pct.max()),
            "naive_filter_extra_removed_pct_max_month": str(naive_pct.idxmax()),
            "naive_filter_extra_removed_rows_total": int(naive["zero_duration_with_distance"].sum()
                                                         + naive["negative_fare_positive_total"].sum()),
        },
        "warehouse": {
            "modelable_zones": int(zones[0]),
            "fct_hourly_zone_demand_rows": int(fct_rows),
            "top50_share_of_clean_pickups": float(top50),
        },
    }
    if dbt_results:
        report["dbt_build"] = dbt_results
    write_report("data_quality.json", report)
    return report


def summarize_dbt_run_results(run_results: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {"by_type": {}, "elapsed_s": run_results.get("elapsed_time")}
    for r in run_results["results"]:
        rtype = r["unique_id"].split(".")[0]
        bucket = out["by_type"].setdefault(rtype, {})
        bucket[r["status"]] = bucket.get(r["status"], 0) + 1
    tests = out["by_type"].get("test", {})
    out["n_tests"] = int(sum(tests.values()))
    out["n_tests_passed"] = int(tests.get("pass", 0))
    return out


# --------------------------------------------------------------------------------------------
# Features
# --------------------------------------------------------------------------------------------

def build_features() -> dict[str, Any]:
    df = features.load_series()
    checks = features.validate_series(df)
    SERIES_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SERIES_PATH, index=False)
    return {**checks, "path": str(SERIES_PATH), "start": str(df["ds"].min()), "end": str(df["ds"].max())}


def load_features() -> pd.DataFrame:
    return pd.read_parquet(SERIES_PATH)


def split_groups(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {g: df[df["series_group"] == g].reset_index(drop=True) for g in GROUPS}


def timeline(df: pd.DataFrame) -> dict[str, list[backtest.Fold]]:
    c = config.CONFIG
    last = df["ds"].max()
    return {
        "val": backtest.make_folds(last, c.val_weeks, skip_weeks=c.test_weeks + c.calib_weeks, phase="val"),
        "calib": backtest.make_folds(last, c.calib_weeks, skip_weeks=c.test_weeks, phase="calib"),
        "test": backtest.make_folds(last, c.test_weeks, phase="test"),
    }


def _span(folds: list[backtest.Fold]) -> dict[str, str]:
    return {"first_origin": str(folds[0].cutoffs[0]), "last_origin": str(folds[-1].cutoffs[-1]),
            "n_folds": len(folds), "n_origins": sum(len(f.cutoffs) for f in folds)}


# --------------------------------------------------------------------------------------------
# Model selection (validation weeks only)
# --------------------------------------------------------------------------------------------

def select_model(df: pd.DataFrame | None = None) -> dict[str, Any]:
    df = load_features() if df is None else df
    tl = timeline(df)
    val = tl["val"]
    report: dict[str, Any] = {"generated_at": _now(), "validation": _span(val), "groups": {}}
    for group, gdf in split_groups(df).items():
        scale = metrics.seasonal_naive_scale(gdf[gdf["ds"] <= val[0].refit_cutoff])
        rows = []
        preds = [backtest.run_seasonal_naive(gdf, val, 168)]
        for cfg in models.CANDIDATES:
            t0 = time.perf_counter()
            p = backtest.run_lgbm(gdf, cfg, val)
            p["model"] = cfg.name
            preds.append(p)
            log.info("validation %s %s done in %.0fs", group, cfg.name, time.perf_counter() - t0)
        summary = metrics.summarize(pd.concat(preds), scale)
        best_name = summary.loc[summary["model"].str.startswith("lgbm"), "model"].iloc[0]
        best = next(c for c in models.CANDIDATES if c.name == best_name)
        # Ablation on the same validation weeks: does the day-2 weather forecast help?
        no_wx = models.LGBMConfig(f"{best.name}_no_weather", objective=best.objective,
                                  use_weather=False, params=best.params)
        p = backtest.run_lgbm(gdf, no_wx, val)
        p["model"] = no_wx.name
        ablation = metrics.summarize(p, scale).iloc[0].to_dict()
        chosen = best
        if ablation["wape"] < float(summary.loc[summary["model"] == best.name, "wape"].iloc[0]):
            chosen = no_wx
        rows = summary.to_dict(orient="records")
        report["groups"][group] = {
            "candidates": rows,
            "weather_ablation": ablation,
            "chosen": chosen.to_dict(),
        }
        with tracking.run(f"selection-{group}", params={"group": group, "chosen": chosen.to_dict()},
                          tags={"stage": "model_selection"}) as (mlflow, _):
            for r in [*rows, ablation]:
                mlflow.log_metrics({f"val_wape.{r['model']}": r["wape"], f"val_mase.{r['model']}": r["mase"]})
    write_report("model_selection.json", report)
    return report


def chosen_configs() -> dict[str, models.LGBMConfig]:
    sel = json.loads((config.REPORTS_DIR / "model_selection.json").read_text())
    return {g: models.LGBMConfig(**v["chosen"]) for g, v in sel["groups"].items()}


# --------------------------------------------------------------------------------------------
# Backtest (calibration + test weeks)
# --------------------------------------------------------------------------------------------

def run_backtest(df: pd.DataFrame | None = None, include_mstl: bool = True) -> dict[str, Any]:
    df = load_features() if df is None else df
    tl = timeline(df)
    calib, test = tl["calib"], tl["test"]
    cfgs = chosen_configs()
    levels = config.CONFIG.interval_levels
    all_preds = []
    report: dict[str, Any] = {
        "generated_at": _now(),
        "protocol": {
            "horizon_h": config.CONFIG.horizon,
            "origin": "23:00 local each day; forecasts the next calendar day (24 hourly values)",
            "refit": "LightGBM re-fitted weekly on all data <= the week's first origin (expanding)",
            "calibration": _span(calib),
            "test": _span(test),
            "hardware": "CPU only, LightGBM n_jobs=4 (i7-11800H laptop)",
        },
        "groups": {},
    }
    timings = {}
    for group, gdf in split_groups(df).items():
        cfg = cfgs[group]
        scale = metrics.seasonal_naive_scale(gdf[gdf["ds"] <= test[0].refit_cutoff])
        t0 = time.perf_counter()
        lg = backtest.run_lgbm(gdf, cfg, calib + test)
        lg["model"] = "lgbm"
        timings[f"{group}_lgbm_s"] = time.perf_counter() - t0
        lg = conformal.add_intervals(lg, levels=levels, calib_days=7 * config.CONFIG.calib_weeks)
        t0 = time.perf_counter()
        frozen = backtest.run_lgbm(gdf, cfg, test, refit=False)
        frozen["model"] = "lgbm_frozen"
        timings[f"{group}_lgbm_frozen_s"] = time.perf_counter() - t0
        parts = [lg[lg["phase"] == "test"], frozen,
                 backtest.run_seasonal_naive(gdf, test, 168), backtest.run_seasonal_naive(gdf, test, 24)]
        if include_mstl:
            t0 = time.perf_counter()
            parts.append(backtest.run_mstl(gdf, test, n_jobs=config.N_THREADS))
            timings[f"{group}_mstl_s"] = time.perf_counter() - t0
        pred = pd.concat(parts, ignore_index=True)
        pred["series_group"] = group
        causal = backtest.assert_backtest_causal(pred)
        test_pred = pred[pred["phase"] == "test"]
        summary = metrics.summarize(test_pred, scale)
        sn = summary.set_index("model").loc["seasonal_naive_168h"]
        lgb_row = summary.set_index("model").loc["lgbm"]
        weekly = backtest.weekly_wape(test_pred)
        wk = weekly.pivot(index="fold", columns="model", values="wape")
        per_series = metrics.per_series_metrics(test_pred[test_pred["model"] == "lgbm"], scale)
        per_series_sn = metrics.per_series_metrics(test_pred[test_pred["model"] == "seasonal_naive_168h"], scale)
        merged = per_series.merge(per_series_sn, on="unique_id", suffixes=("_lgbm", "_sn168"))
        lg_test = lg[lg["phase"] == "test"]
        by_hour = lg_test.assign(hour=lg_test["ds"].dt.hour)
        cov_by_hour = {}
        for lvl in levels:
            inside = (by_hour["y"] >= by_hour[f"lo_{lvl}"]) & (by_hour["y"] <= by_hour[f"hi_{lvl}"])
            cov_by_hour[str(lvl)] = inside.groupby(by_hour["hour"]).mean().round(4).to_dict()
        day1 = None
        if include_mstl:
            # Fair subset: the first origin of each fold, where MSTL is genuinely day-ahead.
            first = test_pred["cutoff"].isin([f.refit_cutoff for f in test])
            day1 = metrics.summarize(test_pred[first], scale).to_dict(orient="records")
        report["groups"][group] = {
            "model_config": cfg.to_dict(),
            "test_metrics_first_origin_of_each_fold": day1,
            "n_series": int(gdf["unique_id"].nunique()),
            "test_metrics": summary.to_dict(orient="records"),
            "lgbm_vs_seasonal_naive_168h": {
                "wape_relative_improvement": float(1 - lgb_row["wape"] / sn["wape"]),
                "mase_relative_improvement": float(1 - lgb_row["mase"] / sn["mase"]),
                "folds_won_wape": int((wk["lgbm"] < wk["seasonal_naive_168h"]).sum()),
                "n_folds": len(wk),
                "series_won_wape": int((merged["wape_lgbm"] < merged["wape_sn168"]).sum()),
                "n_series": len(merged),
            },
            "intervals": conformal.coverage_report(lg_test, levels),
            "interval_coverage_by_hour": cov_by_hour,
            "causality_audit": causal,
        }
        merged.insert(0, "series_group", group)
        merged.to_csv(config.REPORTS_DIR / f"backtest_per_series_{group}.csv", index=False, float_format="%.5f")
        weekly.insert(0, "series_group", group)
        weekly.to_csv(config.REPORTS_DIR / f"backtest_weekly_wape_{group}.csv", index=False, float_format="%.5f")
        all_preds.append(pd.concat([pred, lg[lg["phase"] == "calib"].assign(series_group=group)]))
    report["timings_s"] = {k: round(v, 1) for k, v in timings.items()}
    preds = pd.concat(all_preds, ignore_index=True)
    preds.to_parquet(PRED_PATH, index=False)
    write_report("backtest_metrics.json", report)
    with tracking.run("backtest", params={"protocol": report["protocol"],
                                          "configs": {g: c.to_dict() for g, c in cfgs.items()}},
                      tags={"stage": "backtest"}) as (mlflow, _):
        for group, g in report["groups"].items():
            for row in g["test_metrics"]:
                tracking.log_metrics(mlflow, {k: row[k] for k in ("wape", "mase", "smape")},
                                     prefix=f"test.{group}.{row['model']}.")
            tracking.log_metrics(mlflow, g["intervals"], prefix=f"test.{group}.interval.")
        mlflow.log_artifact(str(config.REPORTS_DIR / "backtest_metrics.json"))
    return report


# --------------------------------------------------------------------------------------------
# Final model
# --------------------------------------------------------------------------------------------

def train_final(df: pd.DataFrame | None = None) -> dict[str, Any]:
    df = load_features() if df is None else df
    cfgs = chosen_configs()
    report: dict[str, Any] = {"generated_at": _now(), "groups": {}}
    for group, gdf in split_groups(df).items():
        cfg = cfgs[group]
        mf = models.model_frame(gdf, cfg)
        t0 = time.perf_counter()
        fcst = models.make_mlforecast(cfg)
        fcst.fit(mf, static_features=["zone"])
        fit_s = time.perf_counter() - t0
        path = config.MODELS_ROOT / f"final_mlforecast_{group}.pkl"
        with path.open("wb") as f:
            pickle.dump(fcst, f)
        booster = fcst.models_["lgbm"]
        imp = pd.Series(booster.booster_.feature_importance("gain"), index=booster.booster_.feature_name())
        imp = (imp / imp.sum()).sort_values(ascending=False)
        report["groups"][group] = {
            "config": cfg.to_dict(),
            "train_rows": len(mf),
            "train_end": str(mf["ds"].max()),
            "fit_seconds": round(fit_s, 1),
            "model_path": str(path),
            "top_features_gain_share": imp.head(10).round(4).to_dict(),
        }
        with tracking.run(f"final-{group}", params={"config": cfg.to_dict(), "train_end": str(mf["ds"].max())},
                          tags={"stage": "final_model"}) as (mlflow, _):
            mlflow.log_metric("fit_seconds", fit_s)
            mlflow.log_artifact(str(path))
    write_report("final_model.json", report)
    return report


# --------------------------------------------------------------------------------------------
# Monitoring: drift + error over time
# --------------------------------------------------------------------------------------------

def drift_monitoring(df: pd.DataFrame | None = None) -> dict[str, Any]:
    df = load_features() if df is None else df
    tl = timeline(df)
    test_start = tl["test"][0].refit_cutoff
    last = df["ds"].max()
    latest_month_start = last.to_period("M").start_time
    cfg = chosen_configs()["zones"]
    zones = split_groups(df)["zones"]
    fcst = models.make_mlforecast(cfg)
    X = fcst.preprocess(models.model_frame(zones, cfg), static_features=["zone"])
    feat_cols = [c for c in X.columns if c not in ("unique_id", "ds", "zone")]
    ref = X[X["ds"] <= test_start]
    cur = X[X["ds"] >= latest_month_start]
    fpsi = drift.feature_psi(ref, cur, feat_cols)
    # Calendar features are known in advance: their PSI only reflects which months the windows cover
    # (a September month vs a multi-month reference), so they are reported but do not raise alerts.
    calendar = {"hour", "dayofweek", "month", "hour_of_week", *features.CALENDAR_COLS}
    fpsi["deterministic_calendar"] = fpsi["feature"].isin(calendar)
    alerting = fpsi[~fpsi["deterministic_calendar"]]
    target_psi = pd.DataFrame(
        [{"unique_id": uid, "psi": drift.psi(ref.loc[ref["unique_id"] == uid, "y"].to_numpy(),
                                             cur.loc[cur["unique_id"] == uid, "y"].to_numpy())}
         for uid in zones["unique_id"].unique()]
    )
    con = config.duckdb_connect(read_only=True)
    try:
        monthly = drift.trip_attribute_psi_by_month(con, ("2024-10-01", "2024-12-01"))
    finally:
        con.close()
    monthly.to_csv(config.REPORTS_DIR / "drift_trip_attributes_monthly.csv", index=False, float_format="%.5f")
    fpsi.to_csv(config.REPORTS_DIR / "drift_feature_psi.csv", index=False, float_format="%.5f")

    preds = pd.read_parquet(PRED_PATH)
    zt = preds[(preds["series_group"] == "zones") & (preds["phase"] == "test")]
    weekly = backtest.weekly_wape(zt)
    wk = weekly.pivot(index="fold", columns="model", values="wape")
    decay = {
        "weekly_wape_lgbm": wk["lgbm"].round(4).tolist(),
        "weekly_wape_lgbm_frozen": wk["lgbm_frozen"].round(4).tolist(),
        "weekly_wape_seasonal_naive_168h": wk["seasonal_naive_168h"].round(4).tolist(),
        "frozen_minus_refit_first4_wape": float((wk["lgbm_frozen"] - wk["lgbm"]).iloc[:4].mean()),
        "frozen_minus_refit_last4_wape": float((wk["lgbm_frozen"] - wk["lgbm"]).iloc[-4:].mean()),
        "lgbm_wape_slope_per_week": float(np.polyfit(np.arange(len(wk)), wk["lgbm"].to_numpy(), 1)[0]),
        "frozen_wape_slope_per_week": float(np.polyfit(np.arange(len(wk)), wk["lgbm_frozen"].to_numpy(), 1)[0]),
    }
    latest = monthly[monthly["source_month"] == latest_month_start.strftime("%Y-%m")]
    report = {
        "generated_at": _now(),
        "thresholds": {"moderate": drift.MODERATE, "significant": drift.SIGNIFICANT},
        "feature_drift": {
            "reference": f"model features {ref['ds'].min()} .. {test_start} (test-period training window)",
            "current": f"{latest_month_start.date()} .. {last}",
            "n_features": len(fpsi),
            "n_alerting_features": len(alerting),
            "n_significant": int((alerting["level"] == "significant").sum()),
            "n_moderate": int((alerting["level"] == "moderate").sum()),
            "max_alerting_psi": float(alerting["psi"].max()),
            "top": alerting.head(8).to_dict(orient="records"),
            "calendar_features_not_alerting": fpsi[fpsi["deterministic_calendar"]].to_dict(orient="records"),
        },
        "target_drift_per_zone": {
            "n_zones": len(target_psi),
            "n_significant": int((target_psi["psi"] >= drift.SIGNIFICANT).sum()),
            "n_moderate": int(((target_psi["psi"] >= drift.MODERATE) & (target_psi["psi"] < drift.SIGNIFICANT)).sum()),
            "median_psi": float(target_psi["psi"].median()),
        },
        "trip_attribute_drift": {
            "baseline": "2024-10 .. 2024-12 clean trips",
            "latest_month": latest.to_dict(orient="records"),
            "months_with_significant_drift": monthly.loc[monthly["level"] == "significant"]
            .groupby("attribute")["source_month"].apply(list).to_dict(),
        },
        "performance_over_time": decay,
    }
    write_report("drift.json", report)
    return report


# --------------------------------------------------------------------------------------------
# Figures
# --------------------------------------------------------------------------------------------

def make_figures() -> list[str]:
    config.FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    preds = pd.read_parquet(PRED_PATH)
    bt = json.loads((config.REPORTS_DIR / "backtest_metrics.json").read_text())
    zt = preds[(preds["series_group"] == "zones") & (preds["phase"] == "test")]
    out = []
    sample = "z161"  # Midtown Center, #2 by volume
    p = config.FIGURES_DIR / "forecast_vs_actual.png"
    plots.forecast_vs_actual(zt, sample, "Midtown Center (zone 161): last test week, day-ahead forecasts", p)
    out.append(str(p))
    summ = pd.DataFrame(bt["groups"]["zones"]["test_metrics"])
    p = config.FIGURES_DIR / "wape_by_model.png"
    plots.wape_by_model(summ, f"Test WAPE, top-50 zones, {bt['protocol']['test']['n_folds']} weekly folds", p)
    out.append(str(p))
    weekly = backtest.weekly_wape(zt)
    monthly = pd.read_csv(config.REPORTS_DIR / "drift_trip_attributes_monthly.csv")
    p = config.FIGURES_DIR / "error_over_time_and_drift.png"
    plots.error_over_time_and_drift(weekly, monthly, p, models=["lgbm", "lgbm_frozen", "seasonal_naive_168h"])
    out.append(str(p))
    return out
