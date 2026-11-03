"""Silver and gold layers.

Silver: typed, cleaned, de-duplicated trips, loaded per month and idempotently
(re-running a month replaces it). Every rejected row lands in
silver.quarantine with the first rule it broke, so nothing disappears silently.

Gold: small, business-ready marts rebuilt from silver on every run.
"""

from __future__ import annotations

from datetime import date
from typing import Callable

import duckdb

from .config import AIRPORT_ZONES, PAYMENT_TYPES, Paths

Log = Callable[[str], None]

# Cleaning rules, in priority order. A row is quarantined with the first rule it breaks.
RULES: list[tuple[str, str]] = [
    ("missing_timestamp", "pickup_at IS NULL OR dropoff_at IS NULL"),
    ("pickup_outside_month", "pickup_at < $month_start OR pickup_at >= $month_end"),
    ("non_positive_duration", "dropoff_at <= pickup_at"),
    ("duration_under_1min", "duration_min < 1"),
    ("duration_over_4h", "duration_min > 240"),
    ("zero_distance", "trip_distance IS NULL OR trip_distance <= 0"),
    ("distance_over_100mi", "trip_distance > 100"),
    ("negative_amount", "fare_amount < 0 OR total_amount < 0"),
    ("fare_below_minimum", "fare_amount < 3"),
    ("total_over_1000", "total_amount > 1000"),
    ("speed_over_80mph", "mph > 80"),
    ("passengers_over_8", "passenger_count > 8"),
]

SILVER_COLUMNS = """
    source_month VARCHAR, vendor_id INTEGER, pickup_at TIMESTAMP, dropoff_at TIMESTAMP,
    passenger_count INTEGER, trip_distance DOUBLE, ratecode_id INTEGER,
    pu_zone_id INTEGER, do_zone_id INTEGER, payment_type INTEGER,
    fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE,
    improvement_surcharge DOUBLE, total_amount DOUBLE, congestion_surcharge DOUBLE, airport_fee DOUBLE,
    duration_min DOUBLE, mph DOUBLE, tip_pct DOUBLE,
    pickup_date DATE, pickup_hour INTEGER, pickup_dow INTEGER
"""


def connect(paths: Paths) -> duckdb.DuckDBPyConnection:
    paths.ensure()
    con = duckdb.connect(str(paths.warehouse))
    con.execute("CREATE SCHEMA IF NOT EXISTS silver; CREATE SCHEMA IF NOT EXISTS gold; CREATE SCHEMA IF NOT EXISTS meta;")
    con.execute(f"CREATE TABLE IF NOT EXISTS silver.trips ({SILVER_COLUMNS})")
    con.execute(f"CREATE TABLE IF NOT EXISTS silver.quarantine (reason VARCHAR, {SILVER_COLUMNS})")
    con.execute("""
        CREATE TABLE IF NOT EXISTS meta.load_stats (
            source_month VARCHAR PRIMARY KEY, bronze_rows BIGINT, silver_rows BIGINT,
            quarantined_rows BIGINT, duplicate_rows BIGINT, loaded_at TIMESTAMP)
    """)
    return con


def load_zones(con: duckdb.DuckDBPyConnection, paths: Paths) -> None:
    con.execute(f"""
        CREATE OR REPLACE TABLE silver.zones AS
        SELECT CAST(LocationID AS INTEGER) AS zone_id, Borough AS borough, Zone AS zone, service_zone
        FROM read_csv_auto('{paths.zones.as_posix()}', header = true)
    """)


def _month_bounds(month: str) -> tuple[date, date]:
    y, m = (int(p) for p in month.split("-"))
    start = date(y, m, 1)
    end = date(y + (m == 12), 1 if m == 12 else m + 1, 1)
    return start, end


def load_silver_month(con: duckdb.DuckDBPyConnection, paths: Paths, month: str, log: Log = print) -> dict:
    """Clean one bronze month into silver (replacing any earlier load of that month)."""
    src = paths.trip_file(month).as_posix()
    start, end = _month_bounds(month)
    reason_sql = "CASE " + " ".join(f"WHEN {cond} THEN '{name}'" for name, cond in RULES) + " ELSE NULL END"
    reason_sql = reason_sql.replace("$month_start", f"TIMESTAMP '{start}'").replace("$month_end", f"TIMESTAMP '{end}'")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE staged AS
        WITH typed AS (
            SELECT
                '{month}' AS source_month,
                CAST(VendorID AS INTEGER) AS vendor_id,
                CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_at,
                CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_at,
                CAST(passenger_count AS INTEGER) AS passenger_count,
                CAST(trip_distance AS DOUBLE) AS trip_distance,
                CAST(RatecodeID AS INTEGER) AS ratecode_id,
                CAST(PULocationID AS INTEGER) AS pu_zone_id,
                CAST(DOLocationID AS INTEGER) AS do_zone_id,
                CAST(payment_type AS INTEGER) AS payment_type,
                CAST(fare_amount AS DOUBLE) AS fare_amount,
                CAST(extra AS DOUBLE) AS extra,
                CAST(mta_tax AS DOUBLE) AS mta_tax,
                CAST(tip_amount AS DOUBLE) AS tip_amount,
                CAST(tolls_amount AS DOUBLE) AS tolls_amount,
                CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge,
                CAST(total_amount AS DOUBLE) AS total_amount,
                CAST(congestion_surcharge AS DOUBLE) AS congestion_surcharge,
                CAST(Airport_fee AS DOUBLE) AS airport_fee
            FROM read_parquet('{src}')
        ), derived AS (
            SELECT *,
                date_diff('second', pickup_at, dropoff_at) / 60.0 AS duration_min,
                CASE WHEN date_diff('second', pickup_at, dropoff_at) > 0
                     THEN trip_distance / (date_diff('second', pickup_at, dropoff_at) / 3600.0) END AS mph,
                -- tips are only recorded for card payments
                CASE WHEN payment_type = 1 AND fare_amount > 0 THEN 100.0 * tip_amount / fare_amount END AS tip_pct,
                CAST(pickup_at AS DATE) AS pickup_date,
                CAST(hour(pickup_at) AS INTEGER) AS pickup_hour,
                CAST(isodow(pickup_at) AS INTEGER) AS pickup_dow
            FROM typed
        )
        SELECT *, {reason_sql} AS reason FROM derived
    """)
    bronze_rows = con.execute("SELECT count(*) FROM staged").fetchone()[0]
    con.execute("BEGIN")
    con.execute("DELETE FROM silver.trips WHERE source_month = ?", [month])
    con.execute("DELETE FROM silver.quarantine WHERE source_month = ?", [month])
    cols = SILVER_COLUMNS_LIST
    con.execute(f"INSERT INTO silver.quarantine SELECT reason, {cols} FROM staged WHERE reason IS NOT NULL")
    con.execute(f"""
        INSERT INTO silver.trips
        SELECT {cols} FROM staged WHERE reason IS NULL
        QUALIFY row_number() OVER (
            PARTITION BY vendor_id, pickup_at, dropoff_at, pu_zone_id, do_zone_id, trip_distance, total_amount
            ORDER BY payment_type
        ) = 1
    """)
    quarantined = con.execute("SELECT count(*) FROM silver.quarantine WHERE source_month = ?", [month]).fetchone()[0]
    silver_rows = con.execute("SELECT count(*) FROM silver.trips WHERE source_month = ?", [month]).fetchone()[0]
    duplicates = bronze_rows - quarantined - silver_rows
    con.execute("""
        INSERT OR REPLACE INTO meta.load_stats VALUES (?, ?, ?, ?, ?, now())
    """, [month, bronze_rows, silver_rows, quarantined, duplicates])
    con.execute("COMMIT")
    con.execute("DROP TABLE staged")
    log(f"{month}: {bronze_rows:,} raw → {silver_rows:,} clean, {quarantined:,} quarantined "
        f"({100 * quarantined / max(1, bronze_rows):.1f}%), {duplicates:,} duplicates")
    return {"month": month, "bronze_rows": bronze_rows, "silver_rows": silver_rows,
            "quarantined_rows": quarantined, "duplicate_rows": duplicates}


SILVER_COLUMNS_LIST = ", ".join(
    part.strip().split()[0] for part in SILVER_COLUMNS.replace("\n", " ").split(",") if part.strip()
)


def build_gold(con: duckdb.DuckDBPyConnection, log: Log = print) -> None:
    """Rebuild every gold mart from silver."""
    airports = ", ".join(f"({zid}, '{name}')" for zid, name in AIRPORT_ZONES.items())
    payments = ", ".join(f"({code}, '{label}')" for code, label in PAYMENT_TYPES.items())
    con.execute(f"CREATE OR REPLACE TABLE gold.dim_airport AS SELECT * FROM (VALUES {airports}) t(zone_id, airport)")
    con.execute(f"CREATE OR REPLACE TABLE gold.dim_payment AS SELECT * FROM (VALUES {payments}) t(payment_type, payment_label)")

    marts = {
        "monthly": """
            SELECT strftime(pickup_date, '%Y-%m') AS month, count(*) AS trips,
                   round(sum(total_amount), 2) AS revenue, round(avg(fare_amount), 2) AS avg_fare,
                   round(avg(total_amount), 2) AS avg_total, round(avg(trip_distance), 2) AS avg_distance,
                   round(avg(duration_min), 1) AS avg_duration_min,
                   round(100.0 * avg(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END), 1) AS card_share_pct,
                   round(avg(tip_pct), 1) AS avg_tip_pct_card
            FROM silver.trips GROUP BY 1 ORDER BY 1""",
        "daily": """
            SELECT pickup_date AS date, count(*) AS trips, round(sum(total_amount), 2) AS revenue,
                   round(avg(fare_amount), 2) AS avg_fare, round(avg(duration_min), 1) AS avg_duration_min,
                   round(avg(mph), 1) AS avg_mph
            FROM silver.trips GROUP BY 1 ORDER BY 1""",
        "hourly_heatmap": """
            WITH days AS (SELECT pickup_dow, count(DISTINCT pickup_date) AS n FROM silver.trips GROUP BY 1)
            SELECT t.pickup_dow AS dow, t.pickup_hour AS hour,
                   round(count(*) / any_value(d.n), 1) AS trips_per_day,
                   round(avg(t.mph), 1) AS avg_mph, round(avg(t.fare_amount), 2) AS avg_fare
            FROM silver.trips t JOIN days d USING (pickup_dow)
            GROUP BY 1, 2 ORDER BY 1, 2""",
        "zone_stats": """
            WITH p AS (SELECT pu_zone_id AS zone_id, count(*) AS pickups, sum(total_amount) AS pickup_revenue,
                              avg(fare_amount) AS avg_fare_from, avg(tip_pct) AS avg_tip_pct_card
                       FROM silver.trips GROUP BY 1),
                 d AS (SELECT do_zone_id AS zone_id, count(*) AS dropoffs FROM silver.trips GROUP BY 1)
            SELECT z.zone_id, z.borough, z.zone, coalesce(p.pickups, 0) AS pickups, coalesce(d.dropoffs, 0) AS dropoffs,
                   round(coalesce(p.pickup_revenue, 0), 2) AS pickup_revenue, round(p.avg_fare_from, 2) AS avg_fare_from,
                   round(p.avg_tip_pct_card, 1) AS avg_tip_pct_card
            FROM silver.zones z LEFT JOIN p USING (zone_id) LEFT JOIN d USING (zone_id)
            ORDER BY pickups DESC""",
        "top_routes": """
            SELECT pz.zone AS pickup_zone, pz.borough AS pickup_borough, dz.zone AS dropoff_zone, dz.borough AS dropoff_borough,
                   count(*) AS trips, round(avg(t.fare_amount), 2) AS avg_fare,
                   round(avg(t.duration_min), 1) AS avg_duration_min, round(avg(t.trip_distance), 2) AS avg_distance
            FROM silver.trips t
            JOIN silver.zones pz ON pz.zone_id = t.pu_zone_id
            JOIN silver.zones dz ON dz.zone_id = t.do_zone_id
            WHERE t.pu_zone_id NOT IN (264, 265) AND t.do_zone_id NOT IN (264, 265)
            GROUP BY ALL ORDER BY trips DESC LIMIT 25""",
        "payment": """
            SELECT coalesce(p.payment_label, 'Other') AS payment, count(*) AS trips,
                   round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS share_pct,
                   round(avg(t.total_amount), 2) AS avg_total, round(avg(t.tip_pct), 1) AS avg_tip_pct
            FROM silver.trips t LEFT JOIN gold.dim_payment p USING (payment_type)
            GROUP BY 1 ORDER BY trips DESC""",
        "airports": """
            SELECT a.airport, 'from airport' AS direction, count(*) AS trips, round(avg(t.fare_amount), 2) AS avg_fare,
                   round(avg(t.total_amount), 2) AS avg_total, round(avg(t.duration_min), 1) AS avg_duration_min,
                   round(avg(t.trip_distance), 2) AS avg_distance, round(avg(t.tip_pct), 1) AS avg_tip_pct_card
            FROM silver.trips t JOIN gold.dim_airport a ON a.zone_id = t.pu_zone_id GROUP BY 1, 2
            UNION ALL
            SELECT a.airport, 'to airport', count(*), round(avg(t.fare_amount), 2), round(avg(t.total_amount), 2),
                   round(avg(t.duration_min), 1), round(avg(t.trip_distance), 2), round(avg(t.tip_pct), 1)
            FROM silver.trips t JOIN gold.dim_airport a ON a.zone_id = t.do_zone_id GROUP BY 1, 2
            ORDER BY 1, 2""",
        "borough_monthly": """
            SELECT strftime(t.pickup_date, '%Y-%m') AS month, coalesce(z.borough, 'Unknown') AS borough,
                   count(*) AS trips, round(sum(t.total_amount), 2) AS revenue
            FROM silver.trips t LEFT JOIN silver.zones z ON z.zone_id = t.pu_zone_id
            GROUP BY 1, 2 ORDER BY 1, revenue DESC""",
        "tip_by_distance": """
            SELECT CASE WHEN trip_distance < 1 THEN '0–1 mi' WHEN trip_distance < 2 THEN '1–2 mi'
                        WHEN trip_distance < 5 THEN '2–5 mi' WHEN trip_distance < 10 THEN '5–10 mi'
                        WHEN trip_distance < 20 THEN '10–20 mi' ELSE '20+ mi' END AS distance_band,
                   min(trip_distance) AS sort_key, count(*) AS card_trips,
                   round(avg(tip_pct), 1) AS avg_tip_pct, round(avg(tip_amount), 2) AS avg_tip
            FROM silver.trips WHERE payment_type = 1 GROUP BY 1 ORDER BY sort_key""",
        "quarantine_summary": """
            SELECT source_month AS month, reason, count(*) AS rows
            FROM silver.quarantine GROUP BY 1, 2 ORDER BY 1, rows DESC""",
    }
    for name, sql in marts.items():
        con.execute(f"CREATE OR REPLACE TABLE gold.{name} AS {sql}")
    log(f"gold: rebuilt {len(marts)} marts")


def transform(paths: Paths, months: list[str], log: Log = print) -> list[dict]:
    con = connect(paths)
    try:
        load_zones(con, paths)
        stats = [load_silver_month(con, paths, m, log) for m in months if paths.trip_file(m).exists()]
        build_gold(con, log)
        return stats
    finally:
        con.close()
