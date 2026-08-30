"""Tiny self-contained data helper for the Airflow demo.

Pure stdlib (no duckdb/pandas) so the stock Airflow image needs no rebuild.
`generate_trips` is deterministic per date — same `ds` always yields the same
batch, which is exactly what makes the pipeline idempotent across re-runs and
backfills. Lives in plugins/, so Airflow puts it on sys.path: `from taxi import ...`.
"""

import random
from collections import Counter

ZONES = ["Manhattan", "Brooklyn", "Queens", "JFK Airport", "Bronx"]
PAYMENTS = ["card", "cash"]


def generate_trips(ds: str) -> list[dict]:
    """Deterministic batch of taxi trips for date `ds` (seeded by the date)."""
    rng = random.Random(ds)
    n = rng.randint(80, 120)
    return [
        {
            "trip_id": f"{ds}-{i:04d}",
            "pickup_zone": rng.choice(ZONES),
            "payment_type": rng.choice(PAYMENTS),
            "fare": round(rng.uniform(5, 80), 2),
        }
        for i in range(n)
    ]


def aggregate(trips: list[dict]) -> dict:
    """Roll trips up into a small summary (trips, revenue, counts per zone)."""
    by_zone = Counter(t["pickup_zone"] for t in trips)
    revenue = round(sum(t["fare"] for t in trips), 2)
    return {"trips": len(trips), "revenue": revenue, "by_zone": dict(by_zone)}
