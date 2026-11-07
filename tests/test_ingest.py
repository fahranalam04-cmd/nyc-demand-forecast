import hashlib

import pytest

from nyc_demand_forecast.ingest import download as dl
from nyc_demand_forecast.ingest import tlc


def test_month_range_crosses_years():
    assert tlc.month_range("2024-11", "2025-02") == ["2024-11", "2024-12", "2025-01", "2025-02"]
    with pytest.raises(ValueError):
        tlc.month_range("2025-02", "2024-11")


def test_schema_changes_detects_added_and_retyped_columns():
    manifest = {
        "2024-12": {"columns": {"a": "int32", "b": "double"}},
        "2025-01": {"columns": {"a": "int64", "b": "double", "cbd_congestion_fee": "double"}},
    }
    (change,) = tlc.schema_changes(manifest, ["2024-12", "2025-01"])
    assert change["added"] == ["cbd_congestion_fee"]
    assert change["retyped"] == {"a": ["int32", "int64"]}


def test_single_part_etag_is_verified_and_multipart_is_unknown(tmp_path):
    f = tmp_path / "x.csv"
    f.write_bytes(b"LocationID,Borough\n1,EWR\n")
    md5 = hashlib.md5(f.read_bytes()).hexdigest()
    assert dl.verify_etag(f, md5) is True
    assert dl.verify_etag(f, "0" * 32) is False
    assert dl.verify_etag(f, "abc-4") is None


def test_fetch_skips_a_verified_local_copy(tmp_path, monkeypatch):
    dest = tmp_path / "yellow_tripdata_2025-01.parquet"
    dest.write_bytes(b"payload")
    entry = {"bytes": 7, "etag": "e-2", "sha256": dl.sha256_file(dest)}
    monkeypatch.setattr(dl, "head", lambda url: dl.RemoteInfo(url, 7, "e-2", None))
    monkeypatch.setattr(dl, "download", lambda *a, **k: pytest.fail("must not re-download"))
    out = tlc.fetch_file("https://example/x", dest, {"2025-01": entry}, key="2025-01")
    assert out["action"] == "skipped"


def test_fetch_redownloads_when_upstream_republished(tmp_path, monkeypatch):
    dest = tmp_path / "f.csv"
    dest.write_bytes(b"old")
    entry = {"bytes": 3, "etag": "old-etag", "sha256": dl.sha256_file(dest)}
    monkeypatch.setattr(dl, "head", lambda url: dl.RemoteInfo(url, 3, "new-etag-2", None))
    calls = []

    def fake_download(url, path, expected_size=None):
        calls.append(url)
        path.write_bytes(b"new")
        return 3

    monkeypatch.setattr(dl, "download", fake_download)
    out = tlc.fetch_file("https://example/f.csv", dest, {"k": entry}, key="k")
    assert calls and out["action"] == "downloaded" and out["etag"] == "new-etag-2"
