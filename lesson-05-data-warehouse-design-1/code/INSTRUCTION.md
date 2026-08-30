# Lesson 05 — dbt demo: the Gold star schema

## What this project is
The lesson-03 dbt project (Bronze + Silver) extended with a **Gold** layer: a full
Kimball star, a denormalized OBT on top, and consistent medallion naming.
Everything is runnable end-to-end with one `dbt build`.

Naming convention: `brz_` (bronze), `slr_` (silver), `gld_` (gold), `seed_` (raw reference CSVs).

```
models/
├── bronze/
│   └── brz_yellow_trips.sql        ← typed table from raw Parquet (source via on-run-start hook)
├── silver/
│   └── slr_yellow_trips.sql        ← cleaned/filtered view
└── gold/
    ├── gld_dim_date.sql            ← generated calendar spine (no source)
    ├── gld_dim_time.sql            ← generated minute spine (no source)
    ├── gld_dim_location.sql        ← SCD2, built from seed_taxi_zone
    ├── gld_dim_vendor.sql          ← thin model over seed_vendor
    ├── gld_dim_rate_code.sql       ← thin model over seed_rate_code
    ├── gld_dim_payment_type.sql    ← thin model over seed_payment_type
    ├── gld_fact_trip.sql           ← star fact: joins all dims to resolve keys
    ├── gld_trip_enriched.sql       ← OBT: the star flattened into one wide table
    └── gld_zone_revenue.sql        ← analytical mart (window functions, from L03)
seeds/
├── seed_taxi_zone.csv              ← 265 NYC taxi zones (TLC reference)
├── seed_vendor.csv                 ← vendor lookup (incl. -1 Unknown)
├── seed_rate_code.csv              ← rate code lookup
└── seed_payment_type.csv           ← payment type lookup
```

## 1. Setup

```bash
cd lesson-05-data-warehouse-design-1/code
uv add dbt-core dbt-duckdb
uv run dbt debug
```

All checks green. The `httpfs` extension in `profiles.yml` lets DuckDB read the
remote CloudFront Parquet. DB file: `dbt_taxi.duckdb`, schema: `main`.

## 2. Build everything

```bash
uv run dbt build
```

`build` runs seeds → models → tests in dependency order (one DAG). Expect
**PASS=47, WARN=0, ERROR=0**. Use bare `dbt run` only after seeds are already loaded.

> Teaching the order: `dbt seed` loads the four `seed_*` CSVs; `dbt run` builds
> `brz → slr → gld_*`; `dbt test` runs not_null/unique/relationships. `build` is all three.

## 3. Walkthrough

For the Kimball 4-step walkthrough with runnable discovery queries, see
`lesson-05-data-warehouse-design-1/demo.md` (that file is the source of truth for
the live-demo narrative). Model-by-model notes are in `schema-design.md`.

## 4. Spot-checks in DuckDB

```bash
uv run python -c "
import duckdb
con = duckdb.connect('dbt_taxi.duckdb')
con.sql('SHOW ALL TABLES').show()
"
```

```sql
-- dim_date: row count + range
SELECT COUNT(*), MIN(full_date), MAX(full_date) FROM main.gld_dim_date;

-- dim_time: verify the key format and rush-hour flag
SELECT time_key, hour, minute, is_rush_hour FROM main.gld_dim_time WHERE hour = 8 LIMIT 5;

-- dim_location: SCD2 columns
SELECT location_key, location_id, zone_name, valid_from, is_current
FROM main.gld_dim_location ORDER BY location_id LIMIT 10;

-- fact: row count + sample
SELECT COUNT(*) FROM main.gld_fact_trip;

-- star query: join fact to dims
SELECT d.full_date, pu.zone_name AS pickup_zone, SUM(f.fare_amount) AS revenue
FROM main.gld_fact_trip f
JOIN main.gld_dim_date     d  ON f.pickup_date_key = d.date_key
JOIN main.gld_dim_location pu ON f.pu_location_key = pu.location_key
GROUP BY 1, 2 ORDER BY revenue DESC LIMIT 10;

-- same answer from the OBT, no joins
SELECT pickup_date, pickup_zone, SUM(fare_amount) AS revenue
FROM main.gld_trip_enriched
GROUP BY 1, 2 ORDER BY revenue DESC LIMIT 10;
```

## 5. Lineage

```bash
uv run dbt docs generate && uv run dbt docs serve
```

The lineage button (bottom-right) shows the connected star: `seed_* → gld_dim_*`,
`source → brz → slr`, all dims + `slr` → `gld_fact_trip` → `gld_trip_enriched`.
Relationship tests appear as test nodes (toggle "tests" in the graph filter).

## 6. What changes in lesson 06
- `slr_yellow_trips` becomes incremental as the pipeline grows.
- `gld_dim_location` seed is replaced by a `dbt snapshot` (managed SCD2).
- `gld_fact_trip` becomes an incremental model (`unique_key = trip_sk`).
- Bronze gains an Airflow DAG for monthly loads.
