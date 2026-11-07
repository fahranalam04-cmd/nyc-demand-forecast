"""Single documented entrypoint.

    uv run nyc-demand-forecast run        # full pipeline via Dagster (ingest -> dbt -> ML -> drift)
    uv run nyc-demand-forecast dbt-parse  # (re)build the dbt manifest Dagster loads
    uv run nyc-demand-forecast ui         # dagster dev UI for the same definitions
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime

from nyc_demand_forecast import config

MODULE = "nyc_demand_forecast.definitions"


def _env() -> dict[str, str]:
    env = dict(os.environ)
    home = config.DATA_ROOT / "dagster_home"
    home.mkdir(parents=True, exist_ok=True)
    (home / "dagster.yaml").touch()  # default local sqlite storage under DAGSTER_HOME
    env.setdefault("DAGSTER_HOME", str(home))
    env.setdefault("FORECAST_DUCKDB_PATH", str(config.DUCKDB_PATH))
    env.setdefault("FORECAST_DUCKDB_TEMP_DIR", str(config.DUCKDB_TEMP_DIR))
    env.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    env.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")
    env.setdefault("OMP_NUM_THREADS", str(config.N_THREADS))
    env.setdefault("MKL_NUM_THREADS", str(config.N_THREADS))
    return env


def _bin(name: str) -> str:
    scripts = os.path.dirname(sys.executable)
    return os.path.join(scripts, name)


def dbt_parse(env: dict[str, str]) -> None:
    subprocess.run([_bin("dbt"), "parse", "--project-dir", str(config.DBT_PROJECT_DIR),
                    "--profiles-dir", str(config.DBT_PROJECT_DIR), "--quiet"], check=True, env=env)


def run_pipeline(select: str | None = None) -> int:
    env = _env()
    t0 = time.perf_counter()
    started = datetime.now(UTC)
    dbt_parse(env)
    if select:  # a subset, e.g. "backtest_results,drift_report" (Click glob-expands "*" on Windows)
        cmd = ["asset", "materialize", "-m", MODULE, "--select", select]
    else:
        cmd = ["job", "execute", "-m", MODULE, "-j", "full_pipeline"]
    proc = subprocess.run([sys.executable, "-m", "dagster", *cmd], env=env)
    wall = time.perf_counter() - t0
    os.environ["DAGSTER_HOME"] = env["DAGSTER_HOME"]
    report = {"started_at": started.isoformat(timespec="seconds"), "command": "dagster " + " ".join(cmd),
              "exit_code": proc.returncode, "wall_clock_s": round(wall, 1),
              "hardware": "i7-11800H laptop, CPU only, 4 threads, DuckDB 3 GB / dbt 2.5 GB memory caps"}
    report.update(_step_stats())
    if not select:
        config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        (config.REPORTS_DIR / "pipeline_run.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in ("exit_code", "wall_clock_s")}))
    return proc.returncode


def _step_stats() -> dict:
    try:
        from dagster import DagsterInstance

        instance = DagsterInstance.get()
        run = instance.get_runs(limit=1)[0]
        steps = instance.get_run_step_stats(run.run_id)
        return {
            "dagster_run_id": run.run_id,
            "dagster_status": str(run.status.value),
            "steps": {s.step_key: round((s.end_time or 0) - (s.start_time or 0), 1) for s in steps},
        }
    except Exception as exc:  # stats are a nice-to-have; never fail the run on them
        return {"step_stats_error": repr(exc)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nyc-demand-forecast", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    run = sub.add_parser("run", help="run the full_pipeline job (or --select a comma-separated subset)")
    run.add_argument("--select", default=None)
    sub.add_parser("dbt-parse")
    sub.add_parser("ui")
    args = parser.parse_args(argv)
    if args.cmd == "run":
        return run_pipeline(args.select)
    env = _env()
    dbt_parse(env)
    if args.cmd == "ui":
        return subprocess.run([sys.executable, "-m", "dagster", "dev", "-m", MODULE], env=env).returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
