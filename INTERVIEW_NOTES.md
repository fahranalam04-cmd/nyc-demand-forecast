# Interview notes: nyc-demand-forecast

All numbers below come from `reports/*.json` and are produced by the pipeline in this repo.

## Why each major design choice

- **DuckDB over Parquet, not a raw table load.** The raw layer is a view over the 24 monthly
  files (`read_parquet(..., union_by_name=true, filename=true)`). DuckDB pushes projections and
  filters into Parquet, so scans over all 93M rows never have to fit in RAM.
  `union_by_name` absorbs the upstream schema drift: `cbd_congestion_fee` appears in 2025-01 and
  `request_source` in 2026-06 (`reports/data_ingestion_trips.json → schema_changes`).
- **One materialised validated-trip table.** Dedup (a GROUP BY over 11 columns) plus rule tagging
  over 93M rows is the most expensive SQL step (it spills to disk under a 2.5 GB cap; the whole
  dbt build takes 95 s, `reports/dbt_build.json → elapsed_s`).
  Doing it once in `int_trips__validated` means the demand fact, the quality report and the tests
  all read the same tagged rows. Each record gets the *first* rule it fails, so the per-rule
  counts add up exactly; `assert_quality_rules_reconcile` checks this and ties the total back to
  the raw source.
- **Narrow quality rules, found by profiling.** Vendor 7 (first seen 2024-12) reports
  dropoff = pickup on all 951,144 of its records, and 935,785 of them have a real distance. Vendor 2
  Flex Fare trips (payment_type 0) carried a negative `fare_amount` with a positive total on
  1,914,022 rows through 2025-11. Those are real pickups. The obvious `duration <= 0 or
  fare < 0` filter would have removed 5.4-7.85% of each month's trips from 2025-05 to 2025-11 and
  about 1.2% a month from 2025-12, an artificial level shift in the target
  (`reports/data_quality.json → vendor_anomalies.naive_filter_extra_removed_pct_by_month`). The rules now remove only negative *totals* and
  zero-duration records with zero distance. A new test, `assert_no_vendor_month_mostly_removed`,
  fails if any rule removes more than half of a vendor's month.
- **Dagster software-defined assets + dagster-dbt.** Each dbt model is its own asset, and dbt
  tests show up as Dagster asset checks. Python checks cover freshness, month completeness,
  series structure, backtest causality, interval coverage and drift. Assets hand off through the
  warehouse and Parquet on the data drive, not pickled IO, and the job runs in-process to keep
  peak RAM in one process on a shared laptop.
- **Day-ahead semantics, made explicit.** Each origin is 23:00 local and forecasts the next
  calendar day. Every lag and rolling window starts at lag ≥ 24 h, so no feature of a 1-24 h-ahead
  forecast can reach past the origin. This makes the recursive and direct strategies identical.
  A perturbation test proves it (`tests/test_leakage.py`).
- **Own rolling-origin loop instead of `mlforecast.cross_validation`.** Weekly refits with daily
  origins, plus explicit assertions in every fold (train ≤ refit cutoff, history ≤ origin, target >
  origin) and a post-hoc audit of every prediction row (`backtest_is_causal` asset check). Easier
  to explain and to test with a spy model.
- **Validation / calibration / test separation.** Four validation weeks choose the objective and
  whether weather is on. The next four weeks are forecast only to warm up conformal calibration.
  The last twelve weekly folds are the test, scored once.
- **Weather as forecasts, never observations.** The model only sees Open-Meteo's archived
  `*_previous_day2` forecasts (issued about 48 h before each target hour, so before the origin).
  Observed weather is kept for the dimension and drift monitoring only.
- **Normalised online conformal intervals.** The score is |y − ŷ| / √(ŷ + 1), the calibration set
  is the trailing 28 days of out-of-sample residuals with target ≤ origin, and the quantile is
  finite-sample corrected. This is cheaper than mlforecast's built-in conformal (which refits per
  window) and causal by construction (tested).
- **PSI drift in two places.** The model feature matrix is compared from the training window to
  the latest month. Raw trip attributes are binned and counted inside DuckDB per month against a
  fixed baseline quarter, so the 91.86M clean trips never leave the warehouse.

## What I tried that didn't work

- **Verifying TLC downloads with the S3 ETag.** The trip files carry multipart ETags
  (`<md5>-4`), and no fixed part size (5-40 MiB or MB) reproduces them, so they were uploaded with
  uneven parts. I record `etag_verified = null` for those and rely on Content-Length, a Parquet
  footer parse and a SHA-256 manifest instead. The zone CSV is single-part and is MD5-verified.
- **Requesting Open-Meteo in local time.** The API applies one fixed UTC offset to a whole
  response (GMT-5 even for EDT dates). I now request GMT and convert in dbt with ICU
  (`to_nyc_local` macro). `assert_weather_forecast_aligned` would catch a 1-hour slip: it fails if
  the forecast matches the observations better at ±1 h than at 0 h.
- **Daily-refit MSTL baseline.** Fitting once per weekly fold already takes 201.7 s for the 50
  zones (`reports/backtest_metrics.json → timings_s.zones_mstl_s`); daily refits are 7× the fits,
  and statsforecast's `forward` costs about as much because it re-decomposes the series. MSTL is now fitted once per weekly fold and forecasts the whole week
  (1-7 day leads), labelled as such, and I also report the fair day-1-only subset.
- **`--select "*"` on Windows.** Click glob-expands CLI arguments on Windows, even through
  `python -m dagster` and even when quoted. The fix is an explicit `full_pipeline` asset job.
- **dbt-duckdb `settings: temp_directory`.** It is re-applied to every cursor and DuckDB refuses to
  change it once it has spilled. It moved to `config_options`.
- **The first dbt build failed `assert_removed_share_within_budget`** (2025-11 was over
  the 10% budget). That failure is what led to the vendor profiling above. After the fix the worst
  month is 3.32% (`reports/data_quality.json → max_monthly_pct_removed`).

## Known weaknesses

- Single study window (24 months) and a single test season (Jul-Sep 2026); twelve folds is
  still a small sample of weeks, and there is no holiday-heavy winter in the test period.
- Weather is a single Midtown grid point; the forecast product is short-lead archived NWP stitched
  per day-2 run, not the exact forecast a dispatcher would have had at 23:00.
- The DST spring-forward hour exists in the spine with zero pickups (none in the test window).
- MSTL is disadvantaged on days 2-7 of each fold (see above). The headline comparison is against
  seasonal naive, which is exact and day-ahead.
- Only yellow taxis; FHV (Uber/Lyft) demand, the larger share of NYC trips, is not modelled.
- CI has never run on GitHub (the repo has not been pushed); it is written for ubuntu + uv.

## Likely interview questions

1. **How do you know there is no leakage?** Three layers. By construction: the origin is 23:00,
   every lag and rolling window starts at lag ≥ 24 h, weather inputs are day-2 forecasts issued
   before the origin, and holiday flags are calendar facts. By test:
   `test_features_at_t_only_use_data_up_to_the_origin` overwrites every target after t0 with random
   values and asserts the features of all target hours ≤ t0 + 24 h are identical, and that the next
   hour does change, so the test can fail. A spy model checks that training never sees rows past the
   refit cutoff and that `predict` never receives `y`. At run time: every fold asserts its bounds,
   and the `backtest_is_causal` asset check audits all 504,000 zone prediction rows, with 0
   violations (`reports/backtest_metrics.json → groups.zones.causality_audit`). Model choice used
   only the 4 validation weeks before the test.
2. **Why WAPE, MASE and sMAPE together?** WAPE is volume-weighted, which is what an operator
   feels in total misallocated cabs. MASE scales each zone's MAE by its own in-sample seasonal-naive
   error, so it compares zones of different size and is directly "skill vs naive". sMAPE is bounded
   but unstable near zero (0/0 is defined as 0), which is why it is reported and not optimised.
3. **What did the data-quality layer actually catch?** 1,595,111 of 93,450,512 rows (1.71%),
   mostly negative totals (1,342,398) and unknown zones 264/265 (185,895)
   (`reports/data_quality.json → totals`). The bigger catch was my own first rule set: the
   removed-share budget test failed on 2025-11, which exposed the vendor 7 and Flex Fare quirks.
   It also records two upstream schema changes, and `assert_weather_forecast_aligned` guards
   against a one-hour time-zone slip in the weather join.
4. **How are the prediction intervals built and are they calibrated?** Split-conformal on the
   score |y − ŷ| / √(ŷ + 1), using the trailing 28 days of out-of-sample residuals known at each
   origin, so the width scales with the forecast level. Marginal coverage is 0.7992 / 0.9484 for
   the 80 / 95% bands over 100,800 zone-hours, and 0.8036 / 0.9499 citywide. It is not
   conditionally calibrated: the 80% band covers 0.686 at 18:00 and 0.907 at 04:00
   (`→ groups.zones.interval_coverage_by_hour`). Next step: Mondrian conformal by hour of day, or
   quantile LightGBM.
5. **Why a global LightGBM across zones instead of 50 models?** One model shares hour-of-week
   and holiday structure across zones, needs 12 weekly fits instead of 600, and lets low-volume
   zones borrow strength. The Tweedie/Poisson objectives handle the different scales without target
   scaling. The citywide total is modelled separately because its mean is about 24× the busiest zone's
   (JFK, z132) in the test window (`reports/backtest_per_series_*.csv → mean_y_lgbm`).
6. **Did weather help?** Barely. On the validation weeks the zone model was 0.1458 WAPE without
   weather and 0.1461 with it, so it ships without. The citywide model improved from 0.0810 to
   0.0789 and keeps it (`reports/model_selection.json → weather_ablation`). In the final citywide
   model `fcst_temp_c` has 0.43% of the split gain, while the 4-week seasonal rolling mean of lag
   168 has 72.6% (`reports/final_model.json → top_features_gain_share`). Seasonal lags already
   explain most of the variance, the test season is summer, and a single grid point misses local
   showers.
7. **How would you detect that the model needs retraining?** `drift_report` tracks three
   signals: feature PSI between the training window and the latest month (largest data-driven
   value 0.036), per-zone target PSI (0 significant, 4 moderate of 50), and weekly WAPE of the
   refit model against a model frozen at the test start. Frozen minus refit was +0.0002 in the
   first 4 test weeks and −0.0023 in the last 4, so there was no decay to fix
   (`reports/drift.json`). In production I would refit weekly anyway (the final fit takes 21.4 s,
   `reports/final_model.json → groups.zones.fit_seconds`) and alert on PSI > 0.25 for a
   data-driven feature or on the weekly WAPE ratio to seasonal naive rising.
8. **What does the orchestration give you over a script?** Asset lineage from raw files to the
   drift report, dbt tests surfaced as asset checks, and selective re-materialisation. For example,
   `nyc-demand-forecast run --select backtest_results,drift_report` reruns just the ML tail. Every
   step's runtime is recorded too (`reports/pipeline_run.json`).
9. **How does it scale past one laptop?** The SQL layer is plain dbt, so swapping dbt-duckdb for
   dbt-bigquery/snowflake changes only profiles and the two DuckDB-specific macros. Ingestion is
   idempotent per month, so it partitions naturally into Dagster monthly partitions. The ML step is
   about 50 series × 17.5K hours; at 1,000s of series I would batch by borough or use mlforecast on
   Spark/Dask.
10. **What would you do next?** Monthly partitions plus a sensor on the TLC page, FHV data, a
    production `day_ahead_forecast` asset that reads the final model, quantile LightGBM as an
    alternative to conformal, and a holiday-season test window.
