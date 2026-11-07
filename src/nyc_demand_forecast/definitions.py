"""Dagster software-defined assets: raw files -> dbt models -> features -> model selection ->
backtest / final model -> drift report, with asset checks at each hand-off.

Run:  dagster job execute -m nyc_demand_forecast.definitions -j full_pipeline
  or: dagster dev -m nyc_demand_forecast.definitions   (then "Materialize all" in the UI)
Assets exchange data through the warehouse / Parquet on the data drive (not pickled IO), and the
job runs in-process so peak RAM stays within one Python process on the shared laptop.
"""

import json
import os
from datetime import datetime

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Definitions,
    MaterializeResult,
    MetadataValue,
    asset,
    asset_check,
    define_asset_job,
    in_process_executor,
    multi_asset,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from nyc_demand_forecast import backtest, config, features, pipeline
from nyc_demand_forecast.ingest import tlc


def _export_env() -> None:
    """dbt runs as a subprocess: hand it the same paths and resource caps as Python."""
    os.environ.setdefault("FORECAST_DUCKDB_PATH", str(config.DUCKDB_PATH))
    os.environ.setdefault("FORECAST_DUCKDB_TEMP_DIR", str(config.DUCKDB_TEMP_DIR))
    os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")
    for var in ("OMP_NUM_THREADS", "MKL_NUM_THREADS"):
        os.environ.setdefault(var, str(config.N_THREADS))


_export_env()
config.ensure_dirs()
DBT_VARS = json.dumps({"start_month": config.CONFIG.start_month, "end_month": config.CONFIG.end_month})
dbt_project = DbtProject(project_dir=config.DBT_PROJECT_DIR, profiles_dir=config.DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()

# --------------------------------------------------------------------------------------------
# Ingestion
# --------------------------------------------------------------------------------------------


@asset(group_name="ingestion", kinds={"python", "duckdb"})
def raw_tlc_trip_files() -> MaterializeResult:
    """Monthly TLC yellow-taxi Parquet files on disk + the raw.yellow_tripdata DuckDB view."""
    r = pipeline.ingest_trips()
    actions = pd.Series([v["last_action"] for v in r["per_month"].values()]).value_counts().to_dict()
    return MaterializeResult(metadata={
        "rows_total": r["rows_total"],
        "n_files": r["n_files"],
        "gb_on_disk": round(r["bytes_total"] / 1e9, 3),
        "actions": MetadataValue.json(actions),
        "schema_changes": MetadataValue.json(r["schema_changes"]),
    })


@asset(group_name="ingestion", kinds={"python", "duckdb"})
def raw_taxi_zone_lookup() -> MaterializeResult:
    r = pipeline.ingest_zones()
    return MaterializeResult(metadata={"rows": r["rows"], "md5_etag_verified": str(r["etag_verified"])})


@multi_asset(
    specs=[AssetSpec("raw_weather_observed", group_name="ingestion", kinds={"python", "duckdb"},
                     description="Open-Meteo observed (reanalysis) hourly weather, GMT."),
           AssetSpec("raw_weather_forecast_d2", group_name="ingestion", kinds={"python", "duckdb"},
                     description="Open-Meteo archived day-2-ahead forecasts (Previous Runs API), GMT.")],
)
def raw_weather():
    r = pipeline.ingest_weather()
    for name in ("observed", "forecast_d2"):
        yield MaterializeResult(asset_key=f"raw_weather_{name}",
                                metadata={"rows": r["duckdb_rows"][f"raw.weather_{name}"]})


@asset_check(asset=raw_tlc_trip_files, description="Every month in the window landed with a plausible row count.")
def tlc_months_complete() -> AssetCheckResult:
    manifest = json.loads((config.TLC_DIR / tlc.MANIFEST_NAME).read_text())
    months = tlc.month_range(config.CONFIG.start_month, config.CONFIG.end_month)
    rows = {m: manifest.get(m, {}).get("rows", 0) for m in months}
    low = {m: n for m, n in rows.items() if n < 2_500_000}
    return AssetCheckResult(passed=not low, metadata={"n_months": len(months), "low_months": MetadataValue.json(low)})


@asset_check(asset=raw_tlc_trip_files, description="Feed freshness: days since the latest real pickup "
             "(same thresholds as the dbt source freshness config: warn > 75 d, error > 120 d).")
def tlc_feed_freshness() -> AssetCheckResult:
    con = config.duckdb_connect(read_only=True)
    try:
        latest = con.execute(
            "select max(tpep_pickup_datetime) from raw.yellow_tripdata where tpep_pickup_datetime <= now()"
        ).fetchone()[0]
    finally:
        con.close()
    age_days = (datetime.now() - latest).days
    severity = AssetCheckSeverity.ERROR if age_days > 120 else AssetCheckSeverity.WARN
    return AssetCheckResult(passed=age_days <= 75, severity=severity,
                            metadata={"latest_pickup": str(latest), "age_days": age_days})


# --------------------------------------------------------------------------------------------
# dbt (staging -> intermediate -> marts; dbt tests surface as Dagster asset checks)
# --------------------------------------------------------------------------------------------


@dbt_assets(manifest=dbt_project.manifest_path)
def warehouse_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    invocation = dbt.cli(["build", "--vars", DBT_VARS], context=context, raise_on_error=False)
    yield from invocation.stream()
    results = pipeline.summarize_dbt_run_results(invocation.get_artifact("run_results.json"))
    pipeline.write_report("dbt_build.json", {"generated_at": pipeline._now(), **results})
    if not invocation.is_successful():
        raise RuntimeError(f"dbt build failed: {results['by_type']}")


# --------------------------------------------------------------------------------------------
# Analytics + ML
# --------------------------------------------------------------------------------------------

MART_DEPS = [AssetKey(["marts", k]) for k in ("fct_hourly_zone_demand", "dim_weather_hourly", "dim_calendar_hourly",
                                              "dim_zones")]


REPORT_DEPS = [AssetKey(["marts", "rpt_trip_quality"]), AssetKey(["marts", "rpt_vendor_anomalies"])]


@asset(group_name="quality", deps=[*REPORT_DEPS, *MART_DEPS],
       kinds={"python"})
def data_quality_report() -> MaterializeResult:
    """reports/data_quality.json: rows removed by each rule, vendor anomalies, dbt test summary."""
    dbt_path = config.REPORTS_DIR / "dbt_build.json"
    dbt_results = json.loads(dbt_path.read_text()) if dbt_path.exists() else None
    r = pipeline.data_quality_report(dbt_results)
    return MaterializeResult(metadata={k: v for k, v in r["totals"].items()})


@asset(group_name="ml", deps=MART_DEPS, kinds={"python", "pandas"})
def training_series() -> MaterializeResult:
    """Top-50 zones + citywide hourly series with known-in-advance exogenous features."""
    r = pipeline.build_features()
    return MaterializeResult(metadata={k: v for k, v in r.items() if k != "path"} | {"path": r["path"]})


@asset_check(asset=training_series, description="Regular hourly index, equal spans, no nulls, 51 series.")
def training_series_structure() -> AssetCheckResult:
    v = features.validate_series(pipeline.load_features())
    ok = v["regular_hourly_index"] and v["same_span_all_series"] and v["null_cells"] == 0 and \
        v["negative_targets"] == 0 and v["n_series"] == config.CONFIG.top_n_zones + 1
    return AssetCheckResult(passed=bool(ok), metadata=v)


@asset(group_name="ml", deps=[training_series], kinds={"lightgbm", "mlflow"})
def model_selection() -> MaterializeResult:
    """Choose the LightGBM objective (and weather on/off) on validation weeks only."""
    r = pipeline.select_model()
    return MaterializeResult(metadata={
        g: MetadataValue.json({"chosen": v["chosen"]["name"],
                               "val_wape": {c["model"]: round(c["wape"], 4) for c in v["candidates"]}})
        for g, v in r["groups"].items()
    })


@asset(group_name="ml", deps=[training_series, model_selection], kinds={"lightgbm", "statsforecast", "mlflow"})
def backtest_results() -> MaterializeResult:
    """Rolling-origin backtest over the test weeks: LightGBM vs seasonal naive vs MSTL, with
    online conformal intervals."""
    r = pipeline.run_backtest()
    z = r["groups"]["zones"]
    return MaterializeResult(metadata={
        "zones_test_wape": MetadataValue.json({m["model"]: round(m["wape"], 4) for m in z["test_metrics"]}),
        "zones_vs_seasonal_naive": MetadataValue.json(z["lgbm_vs_seasonal_naive_168h"]),
        "zones_intervals": MetadataValue.json(z["intervals"]),
        "timings_s": MetadataValue.json(r["timings_s"]),
    })


@asset_check(asset=backtest_results, description="Every forecast target is after its origin and every "
             "model fit is not after its origin (post-hoc causality audit of all prediction rows).")
def backtest_is_causal() -> AssetCheckResult:
    preds = pd.read_parquet(pipeline.PRED_PATH)
    audit = backtest.assert_backtest_causal(preds)
    return AssetCheckResult(passed=audit["violations"] == 0, metadata=audit)


@asset_check(asset=backtest_results,
             description="Empirical interval coverage within 5 pp of nominal (top-50 zones).")
def interval_coverage_near_nominal() -> AssetCheckResult:
    r = json.loads((config.REPORTS_DIR / "backtest_metrics.json").read_text())
    cov = r["groups"]["zones"]["intervals"]
    gaps = {lvl: round(v["empirical_coverage"] - v["nominal"], 4) for lvl, v in cov.items()}
    return AssetCheckResult(passed=all(abs(g) <= 0.05 for g in gaps.values()), severity=AssetCheckSeverity.WARN,
                            metadata={"coverage_minus_nominal": MetadataValue.json(gaps)})


@asset(group_name="ml", deps=[training_series, model_selection], kinds={"lightgbm", "mlflow"})
def trained_model() -> MaterializeResult:
    """Final models fitted on all data (deployable artefacts, logged to MLflow)."""
    r = pipeline.train_final()
    return MaterializeResult(metadata={g: MetadataValue.json({"train_rows": v["train_rows"], "fit_s": v["fit_seconds"]})
                                       for g, v in r["groups"].items()})


@asset(group_name="monitoring", deps=[backtest_results, trained_model], kinds={"python"})
def drift_report() -> MaterializeResult:
    """PSI drift (features, per-zone target, raw trip attributes) + error over time + figures."""
    r = pipeline.drift_monitoring()
    figs = pipeline.make_figures()
    return MaterializeResult(metadata={
        "feature_drift": MetadataValue.json(
            {k: r["feature_drift"][k] for k in ("n_features", "n_significant", "n_moderate")}),
        "target_drift_per_zone": MetadataValue.json(r["target_drift_per_zone"]),
        "figures": MetadataValue.json(figs),
    })


@asset_check(asset=drift_report, description="Warn when any model feature shows significant drift (PSI >= 0.25).")
def no_significant_feature_drift() -> AssetCheckResult:
    r = json.loads((config.REPORTS_DIR / "drift.json").read_text())
    fd = r["feature_drift"]
    return AssetCheckResult(passed=fd["n_significant"] == 0, severity=AssetCheckSeverity.WARN,
                            metadata={"n_significant": fd["n_significant"],
                                      "top": MetadataValue.json(fd["top"][:5])})


# One job for the whole graph (asset checks included). Selecting "*" on the CLI is not portable:
# Click glob-expands arguments on Windows.
full_pipeline = define_asset_job("full_pipeline", selection=AssetSelection.all(),
                                 executor_def=in_process_executor)

defs = Definitions(
    jobs=[full_pipeline],
    assets=[raw_tlc_trip_files, raw_taxi_zone_lookup, raw_weather, warehouse_dbt_assets, data_quality_report,
            training_series, model_selection, backtest_results, trained_model, drift_report],
    asset_checks=[tlc_months_complete, tlc_feed_freshness, training_series_structure, backtest_is_causal,
                  interval_coverage_near_nominal, no_significant_feature_drift],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
    executor=in_process_executor,
)
