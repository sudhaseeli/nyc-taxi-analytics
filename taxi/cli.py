"""Command-line entry point.

    taxi run                       # ingest → silver → gold → quality gate → export
    taxi run --months 2024-01 2024-02
    taxi ingest | transform | quality | export
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from .config import DEFAULT_MONTHS, default_paths
from .export import export
from .ingest import ingest
from .quality import run_checks
from .transform import transform

DASHBOARD_DATA = Path(__file__).resolve().parent.parent / "docs" / "data" / "dashboard.json"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="taxi", description="NYC taxi medallion pipeline on DuckDB")
    p.add_argument("command", choices=["run", "ingest", "transform", "quality", "export"])
    p.add_argument("--months", nargs="+", default=DEFAULT_MONTHS, help="YYYY-MM months to process")
    p.add_argument("--force", action="store_true", help="re-download months that are already loaded")
    args = p.parse_args(argv)
    paths = default_paths().ensure()
    t0 = time.time()

    if args.command in ("run", "ingest"):
        print("▶ ingest (bronze)")
        ingest(paths, args.months, force=args.force)
    if args.command in ("run", "transform"):
        print("▶ transform (silver + gold)")
        transform(paths, args.months)
    if args.command in ("run", "quality"):
        print("▶ data quality")
        ok, _ = run_checks(paths)
        if not ok:
            print("✗ critical data-quality checks failed; dashboard data was not exported", file=sys.stderr)
            return 1
    if args.command in ("run", "export"):
        out = export(paths, DASHBOARD_DATA)
        print(f"▶ exported dashboard data → {out}")
    print(f"done in {time.time() - t0:.1f} s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
