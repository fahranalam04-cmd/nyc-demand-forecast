# nyc-demand-forecast

**Day-ahead hourly NYC yellow-taxi demand forecasts for the 50 busiest taxi zones, built as an
orchestrated pipeline: idempotent ingestion → dbt-duckdb warehouse with data-quality tests →
Dagster assets → LightGBM vs statistical baselines in a rolling-origin backtest → conformal
intervals → drift monitoring.**

## The problem

A dispatcher (or a fleet / pricing team) needs tomorrow's pickups per zone and hour, issued every
night, with an honest error bar. Getting there takes a platform as much as a model. The raw TLC
feed is about 93M rows over 24 months, its schema drifts, and it contains vendor-specific
reporting quirks. A naive cleaning rule would silently delete real demand. The forecast has to be
evaluated the way it would actually run, with no future information, and somebody has to notice
when the inputs or the error start to drift.

## Architecture

```
 NYC TLC CloudFront (24 monthly Parquet, zone CSV)     Open-Meteo (observed + day-2 forecasts)
            │  idempotent download: size check, MD5/ETag where possible, SHA-256 manifest
            ▼
 D:/…/raw/*.parquet ──► DuckDB  raw.*  (zero-copy views over Parquet; 3 GB memory cap)
                              │
                 dbt-duckdb   ▼   (65 data tests, all run on every build)
   staging  stg_tlc__yellow_trips · stg_tlc__taxi_zones · stg_open_meteo__weather_{observed,forecast}
   intermediate  int_trips__validated (dedup + first-failed quality rule) → int_trips__clean
                 → int_demand__zone_hour_counts
   marts    fct_hourly_zone_demand (263 zones × 17,520 h) · dim_zones · dim_calendar_hourly
            · dim_weather_hourly · rpt_trip_quality · rpt_vendor_anomalies
                              │
 Dagster  ────────────────────┼────────────────────────────────────────────────────────────
   raw_* assets ─► dbt assets ─► training_series ─► model_selection ─► backtest_results ─► drift_report
   (+ asset checks: months complete,                (validation      (rolling origin,      (PSI, error
    feed freshness, series structure,                weeks only)       conformal, MLflow)    over time,
    causality audit, coverage, drift)                     └──────► trained_model (MLflow)    figures)
```

Everything heavy (raw Parquet, `warehouse.duckdb`, MLflow `mlflow.db` + artefacts, predictions)
lives under `$ML_DATA_ROOT/forecast` and `$ML_MODELS_ROOT/forecast`. The repo holds only code,
dbt models and small `reports/`.

<!-- RESULTS -->

## Reproduce (3 commands)

```bash
uv sync                                  # Python 3.12 env (set UV_PROJECT_ENVIRONMENT to put it elsewhere)
uv run pytest                            # 22 fast CPU-only tests, no downloads
uv run nyc-demand-forecast run           # full Dagster job: download → dbt build → ML → drift
```

`nyc-demand-forecast run` runs `dbt parse` and then `dagster job execute -j full_pipeline`, and
writes `reports/pipeline_run.json` (wall clock plus per-step timings). `uv run nyc-demand-forecast ui`
opens the same asset graph in `dagster dev`. Paths default to `$ML_DATA_ROOT/forecast` and
`$ML_MODELS_ROOT/forecast` and can be overridden with `FORECAST_DATA_DIR`, `FORECAST_MODELS_DIR`
and `FORECAST_DUCKDB_PATH`. MLflow: `uv run mlflow ui --backend-store-uri sqlite:///<models>/forecast/mlflow.db`.

## Data sources and licenses

| Source | URL | Terms |
|---|---|---|
| NYC TLC Yellow Taxi Trip Records, 2024-10 … 2026-09 (24 monthly Parquet files) | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page (files from `d37ci6vzurychx.cloudfront.net/trip-data/`) | NYC Open Data, public; [NYC Open Data Terms of Use](https://www.nyc.gov/home/terms-of-use.page) |
| TLC Taxi Zone Lookup | `d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv` | as above |
| Open-Meteo Historical Weather API (observed) and Previous Runs API (`*_previous_day2` forecasts) | https://open-meteo.com | free, keyless; data CC BY 4.0, attribution "Weather data by Open-Meteo.com" |
| US federal holidays (observed dates) | Python `holidays` package (MIT), materialised as `dbt/seeds/us_federal_holidays.csv` | MIT |

Row counts, bytes and SHA-256 per month are in `reports/data_ingestion_trips.json`. No accounts,
API keys or paid services are used. Code: MIT (see `LICENSE`).
