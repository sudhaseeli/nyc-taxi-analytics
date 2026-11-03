"""Bronze layer: download raw monthly files exactly as published, incrementally.

Each file is downloaded to a temporary name, verified (it must be a readable
Parquet file with rows), then atomically renamed. A manifest records what was
loaded, from where, its size, row count and SHA-256, so reruns skip months that
are already present and unchanged.
"""

from __future__ import annotations

import hashlib
import json
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import duckdb

from .config import TRIP_URL, ZONE_URL, Paths

Log = Callable[[str], None]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _download(url: str, dest: Path, log: Log, retries: int = 3) -> int:
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "nyc-taxi-analytics/0.1"})
            with urllib.request.urlopen(req, timeout=120) as resp, tmp.open("wb") as out:
                size = 0
                while chunk := resp.read(1 << 20):
                    out.write(chunk)
                    size += len(chunk)
            return size
        except OSError as e:
            log(f"  download failed (attempt {attempt}/{retries}): {e}")
            if attempt == retries:
                tmp.unlink(missing_ok=True)
                raise
            time.sleep(2 * attempt)
    raise RuntimeError("unreachable")


def load_manifest(paths: Paths) -> dict:
    if paths.manifest.exists():
        return json.loads(paths.manifest.read_text(encoding="utf-8"))
    return {"months": {}}


def _save_manifest(paths: Paths, manifest: dict) -> None:
    paths.manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def ingest(paths: Paths, months: list[str], log: Log = print, force: bool = False) -> dict:
    """Download any missing months (and the zone lookup). Returns the manifest."""
    paths.ensure()
    manifest = load_manifest(paths)

    if force or not paths.zones.exists():
        log("zones: downloading taxi_zone_lookup.csv")
        _download(ZONE_URL, paths.zones, log)
        paths.zones.with_suffix(".csv.part").replace(paths.zones)

    for month in months:
        dest = paths.trip_file(month)
        entry = manifest["months"].get(month)
        if not force and entry and dest.exists() and dest.stat().st_size == entry["bytes"]:
            log(f"{month}: already loaded ({entry['rows']:,} rows), skipping")
            continue
        url = TRIP_URL.format(month=month)
        log(f"{month}: downloading {url}")
        size = _download(url, dest, log)
        tmp = dest.with_suffix(dest.suffix + ".part")
        try:
            with duckdb.connect() as con:
                rows = con.execute(f"select count(*) from read_parquet('{tmp.as_posix()}')").fetchone()[0]
        except duckdb.Error as e:
            tmp.unlink(missing_ok=True)
            raise RuntimeError(f"{month}: downloaded file is not valid Parquet: {e}") from e
        if rows == 0:
            tmp.unlink(missing_ok=True)
            raise RuntimeError(f"{month}: downloaded file has no rows")
        tmp.replace(dest)
        manifest["months"][month] = {
            "url": url,
            "bytes": size,
            "rows": rows,
            "sha256": _sha256(dest),
            "loaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        _save_manifest(paths, manifest)
        log(f"{month}: {rows:,} rows, {size / 1e6:.1f} MB")
    return manifest
