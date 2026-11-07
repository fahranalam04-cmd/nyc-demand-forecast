"""Download NYC TLC yellow-taxi monthly Parquet files + the taxi-zone lookup, idempotently."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from nyc_demand_forecast import config
from nyc_demand_forecast.ingest import download as dl

log = logging.getLogger(__name__)

MANIFEST_NAME = "tlc_manifest.json"


def month_range(start: str, end: str) -> list[str]:
    """Inclusive list of 'YYYY-MM' strings."""
    y, m = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    if (y, m) > (ey, em):
        raise ValueError(f"start {start} is after end {end}")
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def trip_url(month: str, service: str = "yellow") -> str:
    return f"{config.TLC_BASE_URL}/{service}_tripdata_{month}.parquet"


def _load_manifest(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def _is_current(entry: dict[str, Any] | None, local: Path, remote: dl.RemoteInfo | None) -> bool:
    """A local file is reusable if it exists, matches the manifest SHA-256 and the remote object
    has not been republished (same size/ETag). Offline (remote=None) we trust the SHA-256 alone."""
    if entry is None or not local.exists():
        return False
    if remote is not None and (remote.size != entry.get("bytes") or remote.etag != entry.get("etag")):
        return False
    return local.stat().st_size == entry.get("bytes") and dl.sha256_file(local) == entry.get("sha256")


def fetch_file(url: str, dest: Path, manifest: dict[str, Any], key: str) -> dict[str, Any]:
    """Download ``url`` to ``dest`` unless an identical verified copy already exists."""
    try:
        remote = dl.head(url)
    except Exception as exc:  # network down: fall back to the local copy if it verifies
        log.warning("HEAD failed for %s (%s); trying local copy", url, exc)
        remote = None
    entry = manifest.get(key)
    if _is_current(entry, dest, remote):
        log.info("skip %s (already downloaded, sha256 verified)", dest.name)
        return {**entry, "action": "skipped"}
    if remote is None:
        raise FileNotFoundError(f"{url} is not available and no verified local copy exists")
    log.info("download %s (%.1f MB)", url, (remote.size or 0) / 1e6)
    dl.download(url, dest, expected_size=remote.size)
    entry = {
        "file": dest.name,
        "url": url,
        "bytes": dest.stat().st_size,
        "etag": remote.etag,
        "etag_verified": dl.verify_etag(dest, remote.etag),
        "sha256": dl.sha256_file(dest),
        "last_modified": remote.last_modified,
        "downloaded_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if entry["etag_verified"] is False:
        dest.unlink(missing_ok=True)
        raise OSError(f"ETag mismatch for {url}; deleted corrupt download")
    return {**entry, "action": "downloaded"}


def parquet_profile(path: Path) -> dict[str, Any]:
    """Row count + schema from the Parquet footer only (no data pages are read)."""
    meta = pq.read_metadata(path)
    schema = pq.read_schema(path)
    return {
        "rows": meta.num_rows,
        "row_groups": meta.num_row_groups,
        "columns": {f.name: str(f.type) for f in schema},
    }


def ingest_tlc(months: list[str] | None = None) -> dict[str, Any]:
    """Land every month in the configured window; update and return the manifest entries."""
    config.ensure_dirs()
    months = months or month_range(config.CONFIG.start_month, config.CONFIG.end_month)
    manifest_path = config.TLC_DIR / MANIFEST_NAME
    manifest = _load_manifest(manifest_path)
    for month in months:
        dest = config.TLC_DIR / f"yellow_tripdata_{month}.parquet"
        entry = fetch_file(trip_url(month), dest, manifest, key=month)
        try:
            entry.update(parquet_profile(dest))  # footer must parse: catches truncated files
        except Exception:
            dest.unlink(missing_ok=True)
            manifest.pop(month, None)
            raise
        entry["month"] = month
        manifest[month] = {k: v for k, v in entry.items() if k != "action"}
        manifest[month]["last_action"] = entry["action"]
        manifest_path.write_text(json.dumps(manifest, indent=2))
    return {m: manifest[m] for m in months}


def ingest_zone_lookup() -> dict[str, Any]:
    """Land the taxi zone lookup CSV (single-part S3 object, so its MD5 ETag is verified)."""
    config.ensure_dirs()
    manifest_path = config.TLC_DIR / MANIFEST_NAME
    manifest = _load_manifest(manifest_path)
    dest = config.TLC_DIR / "taxi_zone_lookup.csv"
    entry = fetch_file(config.ZONE_LOOKUP_URL, dest, manifest, key="taxi_zone_lookup")
    manifest["taxi_zone_lookup"] = {k: v for k, v in entry.items() if k != "action"}
    manifest["taxi_zone_lookup"]["last_action"] = entry["action"]
    manifest_path.write_text(json.dumps(manifest, indent=2))
    return manifest["taxi_zone_lookup"]


def schema_changes(manifest: dict[str, Any], months: list[str]) -> list[dict[str, Any]]:
    """Columns added/removed/retyped between consecutive months (upstream schema drift)."""
    changes = []
    for prev, cur in zip(months, months[1:], strict=False):
        a, b = manifest[prev]["columns"], manifest[cur]["columns"]
        added = sorted(set(b) - set(a))
        removed = sorted(set(a) - set(b))
        retyped = sorted(c for c in set(a) & set(b) if a[c] != b[c])
        if added or removed or retyped:
            changes.append(
                {
                    "month": cur,
                    "added": added,
                    "removed": removed,
                    "retyped": {c: [a[c], b[c]] for c in retyped},
                }
            )
    return changes
