# Resume bullets

**NYC Taxi Demand Forecasting Pipeline | Python · SQL · dbt · DuckDB · Dagster · LightGBM · MLflow**

- Built a Dagster + dbt-duckdb pipeline forecasting next-day hourly taxi pickups for 50 NYC zones; a global LightGBM cut WAPE 27.95% vs seasonal naive (0.1508 vs 0.2093) over a 12-week rolling backtest[^1]
- Modeled 93.45M TLC trips in 13 dbt models with 65 passing data tests; profiling 2 vendor reporting quirks kept real trips a naive filter would have dropped (up to 7.85% of a month)[^2]
- Engineered leakage-safe day-ahead features (all lags ≥ 24 h) and a rolling-origin backtest audited over 504,000 forecast rows with 0 violations; conformal 80/95% intervals covered 79.9%/94.8%[^3]
- Orchestrated ingestion → dbt → training → backtest → PSI drift as Dagster assets with asset checks and MLflow tracking; the full graph runs in 1,731 s on a CPU-only laptop[^4]

## Alternates

- Cut citywide day-ahead WAPE 34.12% vs seasonal naive (0.0828 vs 0.1257) with a Poisson LightGBM using archived day-2 weather forecasts, selected on held-out validation weeks[^5]
- Ingested 1.6 GB of monthly TLC Parquet idempotently (SHA-256 manifest, footer checks) into DuckDB zero-copy views, absorbing 2 upstream schema changes via union_by_name[^6]
- Added drift monitoring that computes monthly PSI over 91.86M clean trips inside DuckDB and tracks refit vs frozen-model error; 0 of 12 data-driven feature columns passed PSI 0.10[^7]

[^1]: `reports/backtest_metrics.json` → `groups.zones.lgbm_vs_seasonal_naive_168h.wape_relative_improvement` (0.2795), `groups.zones.test_metrics` (model `lgbm` wape 0.1508, `seasonal_naive_168h` wape 0.2093), `protocol.test.n_folds` (12), `groups.zones.n_series` (50).
[^2]: `reports/data_quality.json` → `totals.rows_raw` (93,450,512), `vendor_anomalies.naive_filter_extra_removed_pct_max` (7.85, month 2025-11); `reports/dbt_build.json` → `n_tests_passed` (65 of `n_tests` 65), `by_type.model.success` (13).
[^3]: `reports/backtest_metrics.json` → `groups.zones.causality_audit` (rows_checked 504,000, violations 0), `groups.zones.intervals.80.empirical_coverage` (0.7992), `groups.zones.intervals.95.empirical_coverage` (0.9484).
[^4]: `reports/pipeline_run.json` → `wall_clock_s` (1731.4), `dagster_status` (SUCCESS), `hardware`. Raw files were already on disk for this run (ingestion verified and skipped them).
[^5]: `reports/backtest_metrics.json` → `groups.citywide.lgbm_vs_seasonal_naive_168h.wape_relative_improvement` (0.3412), `groups.citywide.test_metrics` (lgbm 0.0828, seasonal_naive_168h 0.1257); `reports/model_selection.json` → `groups.citywide.chosen`.
[^6]: `reports/data_ingestion_trips.json` → `bytes_total` (1,595,328,662), `n_files` (24), `schema_changes` (2 entries: 2025-01, 2026-06).
[^7]: `reports/drift.json` → `feature_drift.n_alerting_features` (12), `feature_drift.n_moderate` (0), `feature_drift.n_significant` (0); `reports/data_quality.json` → `totals.rows_clean` (91,855,401).
