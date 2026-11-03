"""A tiny synthetic month of trips (with deliberately bad rows) so tests run
offline and in CI, without downloading the real 50 MB files."""

from __future__ import annotations

import duckdb
import pytest

from taxi.config import Paths

ZONES = """LocationID,Borough,Zone,service_zone
1,EWR,Newark Airport,EWR
132,Queens,JFK Airport,Airports
138,Queens,LaGuardia Airport,Airports
161,Manhattan,Midtown Center,Yellow Zone
236,Manhattan,Upper East Side North,Yellow Zone
237,Manhattan,Upper East Side South,Yellow Zone
264,Unknown,N/A,N/A
265,N/A,Outside of NYC,N/A
"""

# (minutes after 2024-03-01 08:00, duration_min, distance, fare, tip, total, payment, pu, do, passengers)
GOOD = [
    (0, 10, 1.2, 12.0, 3.0, 19.0, 1, 237, 236, 1),
    (5, 12, 1.0, 10.0, 0.0, 14.5, 2, 236, 237, 2),
    (60, 45, 17.0, 70.0, 15.0, 95.0, 1, 132, 161, 1),
    (120, 30, 9.5, 45.0, 10.0, 66.0, 1, 138, 161, 1),
    (1500, 8, 0.9, 8.6, 2.0, 14.1, 1, 161, 237, 1),     # next day
    (2900, 50, 18.0, 90.0, 20.0, 125.0, 1, 161, 1, 1),   # to Newark
]
BAD = [
    (10, 0.5, 0.3, 4.0, 0.0, 8.0, 1, 237, 236, 1, "duration_under_1min"),
    (20, 10, 0.0, 10.0, 0.0, 14.0, 2, 237, 236, 1, "zero_distance"),
    (30, 10, 1.5, -12.0, 0.0, -18.0, 4, 237, 236, 1, "negative_amount"),
    (40, 300, 5.0, 30.0, 0.0, 40.0, 1, 237, 236, 1, "duration_over_4h"),
    (50, 10, 150.0, 30.0, 0.0, 40.0, 1, 237, 236, 1, "distance_over_100mi"),
    (55, 10, 1.0, 2.0, 0.0, 6.0, 2, 237, 236, 1, "fare_below_minimum"),
    (-60 * 24 * 3, 10, 1.0, 10.0, 0.0, 14.0, 2, 237, 236, 1, "pickup_outside_month"),
    (70, 2, 10.0, 30.0, 0.0, 40.0, 1, 237, 236, 1, "speed_over_80mph"),
]


def _row(m, dur, dist, fare, tip, total, pay, pu, do, pax):
    return (f"TIMESTAMP '2024-03-01 08:00:00' + to_seconds({int(m * 60)})",
            f"TIMESTAMP '2024-03-01 08:00:00' + to_seconds({int((m + dur) * 60)})",
            dist, fare, tip, total, pay, pu, do, pax)


def write_month(path, rows):
    values = []
    for r in rows:
        pu_t, do_t, dist, fare, tip, total, pay, pu, do, pax = _row(*r[:10])
        values.append(f"(2, {pu_t}, {do_t}, {pax}, {dist}, 1, 'N', {pu}, {do}, {pay}, {fare}, 0.0, 0.5, {tip}, 0.0, 1.0, {total}, 2.5, 0.0)")
    duckdb.connect().execute(f"""
        COPY (SELECT * FROM (VALUES {", ".join(values)}) t(
            VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID,
            store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount,
            tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee))
        TO '{path.as_posix()}' (FORMAT parquet)
    """)


@pytest.fixture
def paths(tmp_path):
    p = Paths(tmp_path / "data").ensure()
    p.zones.write_text(ZONES, encoding="utf-8")
    duplicate = GOOD[0]
    write_month(p.trip_file("2024-03"), GOOD + [duplicate] + [b[:10] for b in BAD])
    return p
