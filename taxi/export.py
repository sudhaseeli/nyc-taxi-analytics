"""Export gold marts (small aggregates, not raw trips) as JSON for the dashboard."""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from .config import Paths

TABLES = ["monthly", "daily", "hourly_heatmap", "zone_stats", "top_routes", "payment",
          "airports", "borough_monthly", "tip_by_distance", "quarantine_summary"]


def _plain(v):
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v


def export(paths: Paths, out: Path) -> Path:
    con = duckdb.connect(str(paths.warehouse), read_only=True)
    try:
        data = {}
        for t in TABLES:
            rel = con.execute(f"SELECT * FROM gold.{t}")
            cols = [d[0] for d in rel.description]
            data[t] = [dict(zip(cols, map(_plain, row))) for row in rel.fetchall()]
        data["zone_stats"] = data["zone_stats"][:40]
        rel = con.execute("SELECT * FROM meta.load_stats ORDER BY source_month")
        cols = [d[0] for d in rel.description]
        data["load_stats"] = [dict(zip(cols, map(_plain, row))) for row in rel.fetchall()]
    finally:
        con.close()
    dq = paths.reports / "dq_report.json"
    data["data_quality"] = json.loads(dq.read_text(encoding="utf-8")) if dq.exists() else None
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
    return out
