"""Idempotent HTTP downloads with size + checksum verification.

Integrity checks, strongest first:

* single-part S3 ETag (32-hex MD5, e.g. the zone lookup CSV): the MD5 of the local file must match;
* ``Content-Length`` must equal the bytes written (every file);
* for Parquet, the footer must parse (done by the caller).

The TLC trip files carry *multipart* ETags (``<hash>-<n_parts>``) uploaded with uneven part sizes,
so they cannot be recomputed locally; for those we record ``etag_verified = None`` instead of
pretending. A SHA-256 is stored in the manifest so re-runs skip unchanged files.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import requests

CHUNK = 4 * 1024 * 1024


@dataclass(frozen=True)
class RemoteInfo:
    url: str
    size: int | None
    etag: str | None
    last_modified: str | None


def head(url: str, timeout: float = 30.0) -> RemoteInfo | None:
    """HEAD a URL; ``None`` when the object does not exist (TLC returns 403 for missing months)."""
    resp = requests.head(url, timeout=timeout, allow_redirects=True)
    if resp.status_code in (403, 404):
        return None
    resp.raise_for_status()
    size = resp.headers.get("Content-Length")
    etag = resp.headers.get("ETag")
    return RemoteInfo(
        url=url,
        size=int(size) if size else None,
        etag=etag.strip('"') if etag else None,
        last_modified=resp.headers.get("Last-Modified"),
    )


def _digest(path: Path, algo: str) -> str:
    h = hashlib.new(algo)
    with path.open("rb") as f:
        for block in iter(lambda: f.read(CHUNK), b""):
            h.update(block)
    return h.hexdigest()


def sha256_file(path: Path) -> str:
    return _digest(path, "sha256")


def verify_etag(path: Path, etag: str | None) -> bool | None:
    """True/False when the ETag is a plain MD5; None when it cannot be checked (multipart/absent)."""
    if not etag or "-" in etag or len(etag) != 32:
        return None
    return _digest(path, "md5") == etag.lower()


def download(url: str, dest: Path, expected_size: int | None = None, timeout: float = 120.0) -> int:
    """Stream ``url`` to ``dest`` atomically (``.part`` then rename). Returns bytes written."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    written = 0
    with requests.get(url, stream=True, timeout=timeout) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as f:
            for block in resp.iter_content(chunk_size=CHUNK):
                f.write(block)
                written += len(block)
    if expected_size is not None and written != expected_size:
        tmp.unlink(missing_ok=True)
        raise OSError(f"size mismatch for {url}: got {written}, expected {expected_size}")
    tmp.replace(dest)
    return written
