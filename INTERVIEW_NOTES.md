# Interview notes: nyc-demand-forecast

All numbers below come from `reports/*.json` and are produced by the pipeline in this repo.

## Why each major design choice

- **DuckDB over Parquet, not a raw table load.** The raw layer is a view over the 24 monthly
  files (`read_parquet(..., union_by_name=true, filename=true)`). DuckDB pushes projections and
  filters into Parquet, so a full 93M-row scan takes about 4 s and nothing has to fit in RAM.
  `union_by_name` absorbs the upstream schema drift: `cbd_congestion_fee` appears in 2025-01 and
  `request_source` in 2026-06 (`reports/data_ingestion_trips.json → schema_changes`).
- **One materialised validated-trip table.** Dedup (a GROUP BY over 11 columns) plus rule tagging
  over 93M rows is the only expensive step (about 50 s, spilling to disk under a 2.5 GB cap).
  Doing it once in `int_trips__validated` means the demand fact, the quality report and the tests
  all read the same tagged rows. Each record gets the *first* rule it fails, so the per-rule
  counts add up exactly; `assert_quality_rules_reconcile` checks this and ties the total back to
  the raw source.
- **Narrow quality rules, found by profiling.** Vendor 7 (first seen 2024-12) reports
  dropoff = pickup on all 951,144 of its records, and 935,785 of them have a real distance. Vendor 2
  Flex Fare trips (payment_type 0) carried a negative `fare_amount` with a positive total on
  1,914,022 rows through 2025-11. Those are real pickups. The obvious `duration <= 0 or
  fare < 0` filter deleted 6-8% of demand during 2025 and then stopped, which would have put an
  artificial level shift into the target. The rules now remove only negative *totals* and
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
  fixed baseline quarter, so the 92M clean trips never leave the warehouse.

## What I tried that didn't work

- **Verifying TLC downloads with the S3 ETag.** The trip files carry multipart ETags
  (`<md5>-4`), and no fixed part size (5-40 MiB or MB) reproduces them, so they were uploaded with
  uneven parts. I record `etag_verified = null` for those and rely on Content-Length, a Parquet
  footer parse and a SHA-256 manifest instead. The zone CSV is single-part and is MD5-verified.
- **Requesting Open-Meteo in local time.** The API applies one fixed UTC offset to a whole
  response (GMT-5 even for EDT dates). I now request GMT and convert in dbt with ICU
  (`to_nyc_local` macro). `assert_weather_forecast_aligned` would catch a 1-hour slip: it fails if
  the forecast matches the observations better at ±1 h than at 0 h.
- **Daily-refit MSTL baseline.** One MSTL fit costs about 0.5-0.8 s per series, and statsforecast's
  `forward` costs the same because it re-decomposes. 51 series × 84 origins would have taken
  roughly 25 minutes per run. MSTL is now fitted once per weekly fold and forecasts the whole week
  (1-7 day leads), labelled as such, and I also report the fair day-1-only subset.
- **`--select "*"` on Windows.** Click glob-expands CLI arguments on Windows, even through
  `python -m dagster` and even when quoted. The fix is an explicit `full_pipeline` asset job.
- **dbt-duckdb `settings: temp_directory`.** It is re-applied to every cursor and DuckDB refuses to
  change it once it has spilled. It moved to `config_options`.
- **The first dbt build failed `assert_removed_share_within_budget`** (2025-11 lost 11.2% of rows).
  That failure is what led to the vendor profiling above. After the fix the worst month is 3.3%.

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

1. **How do you know there is no leakage?** <!-- Q1 -->
2. **Why WAPE, MASE and sMAPE together?** WAPE is volume-weighted, which is what an operator
   feels in total misallocated cabs. MASE scales each zone's MAE by its own in-sample seasonal-naive
   error, so it compares zones of different size and is directly "skill vs naive". sMAPE is bounded
   but unstable near zero (0/0 is defined as 0), which is why it is reported and not optimised.
3. **What did the data-quality layer actually catch?** <!-- Q3 -->
4. **How are the prediction intervals built and are they calibrated?** <!-- Q4 -->
5. **Why a global LightGBM across zones instead of 50 models?** One model shares hour-of-week
   and holiday structure across zones, needs 12 weekly fits instead of 600, and lets low-volume
   zones borrow strength. The Tweedie/Poisson objectives handle the different scales without target
   scaling. The citywide total is modelled separately because it is about 50× larger than any zone.
6. **Did weather help?** <!-- Q6 -->
7. **How would you detect that the model needs retraining?** <!-- Q7 -->
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
