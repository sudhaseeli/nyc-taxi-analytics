"""Data-quality gate.

Critical checks stop the pipeline (exit code 1) so bad data never reaches the
dashboard. Warnings are reported but don't block. Results are written to
reports/dq_report.json and reports/dq_report.md.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Callable

import duckdb

from .config import Paths

Log = Callable[[str], None]


@dataclass
class Check:
    name: str
    severity: str  # critical | warn
    description: str
    sql: str  # returns one row: (failures, detail)


@dataclass
class Result:
    name: str
    severity: str
    description: str
    passed: bool
    failures: float
    detail: str


CHECKS = [
    Check("silver_not_empty", "critical", "Silver has trips",
          "SELECT CASE WHEN count(*) = 0 THEN 1 ELSE 0 END, count(*) || ' rows' FROM silver.trips"),
    Check("row_reconciliation", "critical",
          "For every month: raw rows = clean + quarantined + duplicates (nothing lost silently)",
          """SELECT count(*), coalesce(string_agg(source_month, ', '), 'all months balance')
             FROM meta.load_stats s
             WHERE s.bronze_rows <> s.silver_rows + s.quarantined_rows + s.duplicate_rows
                OR s.silver_rows <> (SELECT count(*) FROM silver.trips t WHERE t.source_month = s.source_month)"""),
    Check("no_null_keys", "critical", "No missing timestamps or zone IDs in silver",
          """SELECT count(*), count(*) || ' rows with null keys' FROM silver.trips
             WHERE pickup_at IS NULL OR dropoff_at IS NULL OR pu_zone_id IS NULL OR do_zone_id IS NULL"""),
    Check("duration_in_range", "critical", "Every clean trip lasts 1–240 minutes",
          "SELECT count(*), count(*) || ' out of range' FROM silver.trips WHERE duration_min < 1 OR duration_min > 240"),
    Check("amounts_valid", "critical", "Fares ≥ $3, totals between $0 and $1,000",
          "SELECT count(*), count(*) || ' invalid amounts' FROM silver.trips WHERE fare_amount < 3 OR total_amount < 0 OR total_amount > 1000"),
    Check("unique_trips", "critical", "No duplicate trips in silver",
          """SELECT count(*), count(*) || ' duplicate keys' FROM (
               SELECT 1 FROM silver.trips
               GROUP BY vendor_id, pickup_at, dropoff_at, pu_zone_id, do_zone_id, trip_distance, total_amount
               HAVING count(*) > 1)"""),
    Check("zones_exist", "critical", "Every pickup/drop-off zone exists in the zone lookup (referential integrity)",
          """SELECT count(*), count(*) || ' trips with unknown zone IDs' FROM silver.trips t
             WHERE NOT EXISTS (SELECT 1 FROM silver.zones z WHERE z.zone_id = t.pu_zone_id)
                OR NOT EXISTS (SELECT 1 FROM silver.zones z WHERE z.zone_id = t.do_zone_id)"""),
    Check("gold_reconciles", "critical", "Gold monthly trips and revenue match silver exactly",
          """SELECT CASE WHEN g.trips = s.trips AND abs(g.revenue - s.revenue) < 1 THEN 0 ELSE 1 END,
                    'gold ' || g.trips || ' trips / silver ' || s.trips
             FROM (SELECT sum(trips) trips, sum(revenue) revenue FROM gold.monthly) g,
                  (SELECT count(*) trips, sum(total_amount) revenue FROM silver.trips) s"""),
    Check("quarantine_rate", "warn", "Less than 5% of raw rows quarantined in any month",
          """SELECT count(*), coalesce(string_agg(source_month || ' ' || round(100.0 * quarantined_rows / bronze_rows, 1) || '%', ', '), 'all months under 5%')
             FROM meta.load_stats WHERE quarantined_rows > 0.05 * bronze_rows"""),
    Check("daily_completeness", "warn", "Every day of every loaded month has trips",
          """WITH bounds AS (SELECT min(pickup_date) lo, max(pickup_date) hi FROM silver.trips),
                  days AS (SELECT unnest(generate_series(lo, hi, INTERVAL 1 DAY))::DATE d FROM bounds)
             SELECT count(*), coalesce(string_agg(strftime(d, '%Y-%m-%d'), ', '), 'no gaps')
             FROM days WHERE d NOT IN (SELECT DISTINCT pickup_date FROM silver.trips)"""),
    Check("unknown_zone_share", "warn", "Under 3% of trips start in an unknown zone (264/265)",
          """SELECT CASE WHEN avg(CASE WHEN pu_zone_id IN (264, 265) THEN 1 ELSE 0 END) > 0.03 THEN 1 ELSE 0 END,
                    round(100 * avg(CASE WHEN pu_zone_id IN (264, 265) THEN 1 ELSE 0 END), 2) || '% unknown'
             FROM silver.trips"""),
    Check("tip_rate_plausible", "warn", "Average card tip is between 10% and 30% of the fare",
          """SELECT CASE WHEN avg(tip_pct) BETWEEN 10 AND 30 THEN 0 ELSE 1 END, round(avg(tip_pct), 1) || '% average'
             FROM silver.trips WHERE tip_pct IS NOT NULL"""),
]


def run_checks(paths: Paths, log: Log = print) -> tuple[bool, list[Result]]:
    con = duckdb.connect(str(paths.warehouse), read_only=True)
    results: list[Result] = []
    try:
        for c in CHECKS:
            try:
                failures, detail = con.execute(c.sql).fetchone()
                failures = failures or 0
            except duckdb.Error as e:
                failures, detail = 1, f"check could not run: {e}"
            r = Result(c.name, c.severity, c.description, failures == 0, float(failures), str(detail))
            results.append(r)
            mark = "PASS" if r.passed else ("FAIL" if c.severity == "critical" else "WARN")
            log(f"  [{mark}] {c.name}: {r.detail}")
    finally:
        con.close()
    ok = all(r.passed for r in results if r.severity == "critical")
    _write_report(paths, ok, results)
    return ok, results


def _write_report(paths: Paths, ok: bool, results: list[Result]) -> None:
    paths.reports.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    (paths.reports / "dq_report.json").write_text(
        json.dumps({"generated_at": stamp, "passed": ok, "checks": [asdict(r) for r in results]}, indent=2),
        encoding="utf-8")
    lines = [f"# Data quality report", "", f"Generated {stamp} · **{'PASSED' if ok else 'FAILED'}**", "",
             "| Check | Severity | Result | Detail |", "|---|---|---|---|"]
    for r in results:
        res = "✅ pass" if r.passed else ("❌ fail" if r.severity == "critical" else "⚠️ warn")
        lines.append(f"| {r.name} | {r.severity} | {res} | {r.detail} |")
    (paths.reports / "dq_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
