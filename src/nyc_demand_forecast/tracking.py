"""MLflow tracking: local SQLite backend + artifacts on the data drive (no server needed)."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any

from nyc_demand_forecast import config

EXPERIMENT = "nyc-demand-forecast"


def tracking_uri() -> str:
    return os.environ.get("MLFLOW_TRACKING_URI", f"sqlite:///{config.MLFLOW_DB.resolve().as_posix()}")


def setup():
    os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")
    import mlflow

    config.ensure_dirs()
    mlflow.set_tracking_uri(tracking_uri())
    if mlflow.get_experiment_by_name(EXPERIMENT) is None:
        config.MLFLOW_ARTIFACTS.mkdir(parents=True, exist_ok=True)
        mlflow.create_experiment(EXPERIMENT, artifact_location=config.MLFLOW_ARTIFACTS.resolve().as_uri())
    mlflow.set_experiment(EXPERIMENT)
    return mlflow


def _flat(prefix: str, d: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(_flat(f"{key}.", v))
        else:
            out[key] = v
    return out


@contextmanager
def run(name: str, params: dict[str, Any] | None = None, tags: dict[str, str] | None = None):
    mlflow = setup()
    with mlflow.start_run(run_name=name, tags=tags) as active:
        if params:
            mlflow.log_params({k: str(v)[:500] for k, v in _flat("", params).items()})
        yield mlflow, active


def log_metrics(mlflow, metrics: dict[str, Any], prefix: str = "") -> None:
    flat = {k: float(v) for k, v in _flat(prefix, metrics).items() if isinstance(v, int | float)}
    mlflow.log_metrics({k.replace("%", "pct"): v for k, v in flat.items()})
