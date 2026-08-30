{{ config(materialized="table") }}

SELECT
    CAST(VendorID              AS INTEGER)       AS vendor_id,
    CAST(tpep_pickup_datetime  AS TIMESTAMP)     AS tpep_pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP)     AS tpep_dropoff_datetime,
    CAST(passenger_count       AS SMALLINT)      AS passenger_count,
    CAST(trip_distance         AS DOUBLE)        AS trip_distance,
    CAST(RatecodeID            AS SMALLINT)      AS ratecode_id,
    CAST(store_and_fwd_flag    AS VARCHAR)       AS store_and_fwd_flag,
    CAST(PULocationID          AS SMALLINT)      AS pu_location_id,
    CAST(DOLocationID          AS SMALLINT)      AS do_location_id,
    CAST(payment_type          AS SMALLINT)      AS payment_type,
    CAST(fare_amount           AS DECIMAL(10,2)) AS fare_amount,
    CAST(extra                 AS DECIMAL(10,2)) AS extra,
    CAST(mta_tax               AS DECIMAL(10,2)) AS mta_tax,
    CAST(tip_amount            AS DECIMAL(10,2)) AS tip_amount,
    CAST(tolls_amount          AS DECIMAL(10,2)) AS tolls_amount,
    CAST(improvement_surcharge AS DECIMAL(10,2)) AS improvement_surcharge,
    CAST(total_amount          AS DECIMAL(10,2)) AS total_amount,
    CAST(congestion_surcharge  AS DECIMAL(10,2)) AS congestion_surcharge,
    CAST("Airport_fee"         AS DECIMAL(10,2)) AS airport_fee
FROM {{ source("nyc_tlc", "yellow_trips_raw") }}
