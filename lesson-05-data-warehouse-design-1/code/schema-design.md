# Star Schema Design: NYC Yellow Taxi DWH

Medallion layering: `brz_` (raw, typed) → `slr_` (cleaned) → `gld_` (star + OBT).
Raw reference data lives in `seed_*` CSVs. Logical names below map to model names
as `fact_trip` = `gld_fact_trip`, `dim_x` = `gld_dim_x`.

## Fact table: `gld_fact_trip`

One row per taxi trip. All measures are additive. Built from `slr_yellow_trips`,
**joining every dimension** to resolve/conform its key (wires the star in lineage
and guarantees no orphan FKs).

| Column | Type | Notes |
|---|---|---|
| trip_sk | VARCHAR (PK) | Surrogate key (md5 of the natural key) |
| pickup_date_key | INTEGER (FK) | → dim_date.date_key |
| pickup_time_key | INTEGER (FK) | → dim_time.time_key |
| dropoff_date_key | INTEGER (FK) | → dim_date.date_key |
| pu_location_key | VARCHAR (FK) | → dim_location.location_key (SCD2) |
| do_location_key | VARCHAR (FK) | → dim_location.location_key (SCD2) |
| rate_code_key | INTEGER (FK) | → dim_rate_code.rate_code_id |
| payment_type_key | INTEGER (FK) | → dim_payment_type.payment_type_id |
| vendor_key | INTEGER (FK) | → dim_vendor.vendor_id |
| fare_amount | DECIMAL(10,2) | Base fare |
| tip_amount | DECIMAL(10,2) | Tip |
| tolls_amount | DECIMAL(10,2) | Tolls |
| total_amount | DECIMAL(10,2) | Total charged |
| trip_distance | DOUBLE | Miles |
| duration_sec | BIGINT | Dropoff - pickup in seconds |
| passenger_count | SMALLINT | Riders |

**Natural key** (used for dedup + surrogate): `pickup_datetime + dropoff_datetime + pu_location_id + do_location_id + fare_amount + total_amount`

> The narrower `pickup_datetime + pu_location_id + fare_amount` collides on ~3.8k rows — it does not hold the grain (one row per trip). The six-column key above is unique across the Jan-2024 dataset.

---

## Dimensions

### gld_dim_location (SCD Type 2)

265 NYC taxi zones, built from `seed_taxi_zone`. SCD2 because zone boundaries change
(e.g., airport zone redefinition).

| Column | Type | Notes |
|---|---|---|
| location_key | VARCHAR (PK) | md5(location_id + valid_from) |
| location_id | INTEGER | TLC zone ID 1–265 |
| borough | VARCHAR | Manhattan, Brooklyn, etc. |
| zone_name | VARCHAR | e.g. "JFK Airport" |
| service_zone | VARCHAR | Boro Taxi, Yellow Zone, etc. |
| valid_from | DATE | When this row became active |
| valid_to | DATE | 9999-12-31 if current |
| is_current | BOOLEAN | TRUE = current row |

### gld_dim_date

Calendar from 2009-01-01 to 2030-12-31. Generated via recursive CTE.

| Column | Notes |
|---|---|
| date_key (PK) | YYYYMMDD integer |
| full_date | DATE |
| year, month, day | SMALLINT each |
| day_of_week | 0=Sunday … 6=Saturday |
| day_name | 'Monday' … |
| is_weekend | BOOLEAN |
| quarter | 1–4 |

### gld_dim_time

1440 rows (one per minute). Static, generated via CTE.

| Column | Notes |
|---|---|
| time_key (PK) | HHMM integer (0–2359). NB: integer division `//` — DuckDB `/` is float |
| hour, minute | SMALLINT |
| is_rush_hour | 07–09, 16–18 |
| time_of_day | Night/Morning/Afternoon/Evening |

### gld_dim_vendor, gld_dim_rate_code, gld_dim_payment_type

Small lookup tables — SCD Type 1 (overwrite on change). The domain lives in a
**seed** (`seed_vendor.csv`, `seed_rate_code.csv`, `seed_payment_type.csv`, sourced
from the TLC data dictionary, each with a `-1` Unknown member); a thin `gld_dim_*`
model types it and exposes it as the dimension. Editing the domain = editing the CSV.

---

## OBT: `gld_trip_enriched`

The star flattened into One Big Table — every dimension attribute (borough, zone,
vendor name, payment label, day/hour context) folded into the fact, queryable with
zero joins. Built **from** the star (`gld_fact_trip` + dims), not instead of it. The
columnar-era denormalized alternative to the star (Tier 2).

---

## Bus Matrix

|  | gld_fact_trip |
|---|---|
| gld_dim_date (pickup) | ✓ |
| gld_dim_date (dropoff) | ✓ |
| gld_dim_time (pickup) | ✓ |
| gld_dim_location (PU) | ✓ |
| gld_dim_location (DO) | ✓ |
| gld_dim_vendor | ✓ |
| gld_dim_rate_code | ✓ |
| gld_dim_payment_type | ✓ |

---

## Design decisions

**Why SCD2 for dim_location?**  
NYC rezones taxi areas occasionally. Without SCD2, a historical trip from zone "East Village" 
would point to the current (potentially renamed/redrawn) zone — incorrect historical reporting.

**Why DECIMAL(10,2) for all monetary columns?**  
IEEE 754 float64: `0.1 + 0.2 = 0.30000000000000004`. Unacceptable for financial aggregates.  
DECIMAL(10,2) guarantees exact representation.

**Why a hash surrogate key (md5), not serial integer?**  
Serial integers require coordination across parallel loads (sequences or locking).  
An md5 of the natural key is deterministic, idempotent, and works across both DuckDB
and Spark. The natural key must hold the grain — see the 6-column key above.

**Kimball, not Inmon:**  
Star schema (denormalized dims) rather than 3NF + data marts. Reason: our consumers are 
analysts running ad-hoc GROUP BY — they benefit from denormalized dims (fewer joins).  
Inmon would add complexity without benefit at our scale.
