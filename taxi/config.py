"""Paths and constants. Everything lives under one project data directory."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

TRIP_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

DEFAULT_MONTHS = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"]

# Taxi zone IDs used for airport analysis
AIRPORT_ZONES = {1: "EWR", 132: "JFK", 138: "LGA"}

PAYMENT_TYPES = {0: "Flex fare", 1: "Credit card", 2: "Cash", 3: "No charge", 4: "Dispute", 5: "Unknown", 6: "Voided"}


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def bronze(self) -> Path:
        return self.root / "bronze"

    @property
    def warehouse(self) -> Path:
        return self.root / "warehouse.duckdb"

    @property
    def manifest(self) -> Path:
        return self.bronze / "_manifest.json"

    @property
    def zones(self) -> Path:
        return self.bronze / "taxi_zone_lookup.csv"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

    def trip_file(self, month: str) -> Path:
        return self.bronze / f"yellow_tripdata_{month}.parquet"

    def ensure(self) -> "Paths":
        self.bronze.mkdir(parents=True, exist_ok=True)
        self.reports.mkdir(parents=True, exist_ok=True)
        return self


def default_paths() -> Paths:
    root = os.environ.get("TAXI_DATA_DIR")
    return Paths(Path(root) if root else Path(__file__).resolve().parent.parent / "data")
