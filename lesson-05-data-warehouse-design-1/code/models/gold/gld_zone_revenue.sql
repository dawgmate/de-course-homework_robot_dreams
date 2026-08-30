{{ config(materialized="table") }}

WITH trip_totals AS (
    SELECT
        pu_location_id,
        COUNT(*)         AS trip_count,
        SUM(fare_amount) AS total_revenue,
        AVG(fare_amount) AS avg_fare,
        SUM(tip_amount)  AS total_tips
    FROM {{ ref("slr_yellow_trips") }}
    GROUP BY pu_location_id
),
with_zones AS (
    SELECT
        t.pu_location_id,
        z.Borough    AS borough,
        z.Zone       AS zone,
        z.service_zone,
        t.trip_count,
        t.total_revenue,
        t.avg_fare,
        t.total_tips
    FROM trip_totals t
    JOIN {{ ref("seed_taxi_zone") }} z ON t.pu_location_id = z.LocationID
)
SELECT
    *,
    SUM(total_revenue) OVER (
        PARTITION BY borough ORDER BY pu_location_id
    )                  AS cumulative_borough_revenue,
    RANK() OVER (
        PARTITION BY borough ORDER BY total_revenue DESC
    )                  AS revenue_rank_in_borough
FROM with_zones
