{{ config(materialized="view") }}

SELECT *
FROM {{ ref("brz_yellow_trips") }}
WHERE fare_amount           >= 0
  AND trip_distance         >= 0
  AND tpep_pickup_datetime  IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND pu_location_id        IS NOT NULL
  AND do_location_id        IS NOT NULL
