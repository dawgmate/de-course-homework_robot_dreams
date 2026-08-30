{{ config(materialized='table') }}

-- One Big Table (OBT): the star flattened. Every dimension attribute folded
-- into the fact so BI tools query one wide table with no joins. This is the
-- columnar-era denormalized alternative to the star (L05, Tier 2) — built FROM
-- the star, not instead of it.

SELECT
    f.trip_sk,

    -- Pickup date / time context (from dim_date, dim_time)
    ddp.full_date                       AS pickup_date,
    ddp.day_name                        AS pickup_day_name,
    ddp.is_weekend                      AS pickup_is_weekend,
    dt.hour                             AS pickup_hour,
    dt.is_rush_hour                     AS pickup_is_rush_hour,
    dt.time_of_day                      AS pickup_time_of_day,
    ddd.full_date                       AS dropoff_date,

    -- Location context (role-playing dim_location)
    pu.borough                          AS pickup_borough,
    pu.zone_name                        AS pickup_zone,
    do_.borough                         AS dropoff_borough,
    do_.zone_name                       AS dropoff_zone,

    -- Descriptive attributes (lookup dims)
    dv.vendor_name,
    dr.rate_code_description,
    dp.payment_type_description,

    -- Measures
    f.fare_amount,
    f.tip_amount,
    f.tolls_amount,
    f.total_amount,
    f.trip_distance,
    f.duration_sec,
    f.passenger_count

FROM {{ ref('gld_fact_trip') }} f
LEFT JOIN {{ ref('gld_dim_date') }} ddp        ON ddp.date_key        = f.pickup_date_key
LEFT JOIN {{ ref('gld_dim_date') }} ddd        ON ddd.date_key        = f.dropoff_date_key
LEFT JOIN {{ ref('gld_dim_time') }} dt         ON dt.time_key         = f.pickup_time_key
LEFT JOIN {{ ref('gld_dim_location') }} pu     ON pu.location_key     = f.pu_location_key
LEFT JOIN {{ ref('gld_dim_location') }} do_    ON do_.location_key    = f.do_location_key
LEFT JOIN {{ ref('gld_dim_vendor') }} dv       ON dv.vendor_id        = f.vendor_key
LEFT JOIN {{ ref('gld_dim_rate_code') }} dr    ON dr.rate_code_id     = f.rate_code_key
LEFT JOIN {{ ref('gld_dim_payment_type') }} dp ON dp.payment_type_id  = f.payment_type_key
