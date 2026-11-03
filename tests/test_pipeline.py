import json

import duckdb

from taxi.export import export
from taxi.quality import run_checks
from taxi.transform import transform

from conftest import BAD, GOOD


def q(paths, sql):
    con = duckdb.connect(str(paths.warehouse), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_clean_rows_reach_silver_and_bad_rows_are_quarantined_with_reasons(paths):
    stats = transform(paths, ["2024-03"], log=lambda *_: None)[0]
    assert stats["bronze_rows"] == len(GOOD) + 1 + len(BAD)
    assert stats["silver_rows"] == len(GOOD)
    assert stats["duplicate_rows"] == 1
    assert stats["quarantined_rows"] == len(BAD)
    reasons = {r for (r,) in q(paths, "SELECT reason FROM silver.quarantine")}
    assert reasons == {b[10] for b in BAD}


def test_rerunning_a_month_is_idempotent(paths):
    transform(paths, ["2024-03"], log=lambda *_: None)
    transform(paths, ["2024-03"], log=lambda *_: None)
    assert q(paths, "SELECT count(*) FROM silver.trips")[0][0] == len(GOOD)
    assert q(paths, "SELECT count(*) FROM silver.quarantine")[0][0] == len(BAD)


def test_derived_columns(paths):
    transform(paths, ["2024-03"], log=lambda *_: None)
    row = q(paths, """SELECT duration_min, round(mph, 2), round(tip_pct, 1), pickup_hour, pickup_dow
                      FROM silver.trips WHERE pu_zone_id = 237 AND do_zone_id = 236""")[0]
    assert row == (10.0, 7.2, 25.0, 8, 5)  # 1.2 mi in 10 min; $3 tip on $12; 8 am Friday
    cash_tip = q(paths, "SELECT tip_pct FROM silver.trips WHERE payment_type = 2")[0][0]
    assert cash_tip is None  # cash tips aren't recorded, so no tip % for cash


def test_gold_marts(paths):
    transform(paths, ["2024-03"], log=lambda *_: None)
    month, trips, revenue = q(paths, "SELECT month, trips, revenue FROM gold.monthly")[0]
    assert (month, trips) == ("2024-03", len(GOOD))
    assert revenue == round(sum(g[5] for g in GOOD), 2)
    airports = {(a, d): n for a, d, n in q(paths, "SELECT airport, direction, trips FROM gold.airports")}
    assert airports == {("JFK", "from airport"): 1, ("LGA", "from airport"): 1, ("EWR", "to airport"): 1}
    routes = {(p, d): n for p, d, n in q(paths, "SELECT pickup_zone, dropoff_zone, trips FROM gold.top_routes")}
    assert routes[("Upper East Side South", "Upper East Side North")] == 1
    assert sum(routes.values()) == len(GOOD)  # every clean trip has known zones here


def test_quality_gate_passes_on_clean_data(paths):
    transform(paths, ["2024-03"], log=lambda *_: None)
    ok, results = run_checks(paths, log=lambda *_: None)
    failed = [r.name for r in results if not r.passed and r.severity == "critical"]
    assert ok, f"critical checks failed: {failed}"
    report = json.loads((paths.reports / "dq_report.json").read_text())
    assert report["passed"] is True


def test_quality_gate_catches_corruption(paths):
    transform(paths, ["2024-03"], log=lambda *_: None)
    con = duckdb.connect(str(paths.warehouse))
    con.execute("INSERT INTO silver.trips SELECT * FROM silver.trips LIMIT 1")  # sneak in a duplicate
    con.execute("UPDATE silver.trips SET duration_min = 999 WHERE pu_zone_id = 132")  # and a bad duration
    con.close()
    ok, results = run_checks(paths, log=lambda *_: None)
    failed = {r.name for r in results if not r.passed}
    assert not ok
    assert {"unique_trips", "duration_in_range", "row_reconciliation", "gold_reconciles"} <= failed


def test_export_produces_dashboard_json(paths, tmp_path):
    transform(paths, ["2024-03"], log=lambda *_: None)
    run_checks(paths, log=lambda *_: None)
    out = export(paths, tmp_path / "dashboard.json")
    data = json.loads(out.read_text())
    assert data["monthly"][0]["trips"] == len(GOOD)
    assert data["data_quality"]["passed"] is True
    assert {"hourly_heatmap", "top_routes", "airports", "load_stats"} <= data.keys()
