-- Bronze — типізована таблиця, явні CAST для всіх колонок (як у L05).
--
-- ЩО ДОДАНО ДЛЯ ОРКЕСТРАЦІЇ (у L05 було просто materialized="table"):
--   * `pickup_date` — поле-партиція: за ним dbt переписує рівно один день;
--   * `_loaded_at`  — коли партицію завантажили востаннє (audit-колонка);
--   * materialized='incremental' + unique_key='pickup_date' + delete+insert:
--     dbt видаляє партицію дня і вставляє її наново, тож повторний прогін
--     того самого `ds` не дублює рядки;
--   * фільтр `WHERE ... = var('ds')` — Airflow передає logical date, і модель
--     читає з Parquet РІВНО одну добу замість усього місяця. Без `--vars`
--     (звичайний `dbt build`) фільтра немає — збереться весь glob.

{{ config(
    materialized="incremental",
    unique_key="pickup_date",
    incremental_strategy="delete+insert",
) }}

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
    CAST("Airport_fee"         AS DECIMAL(10,2)) AS airport_fee,

    -- поля партиціонування та аудиту
    CAST(tpep_pickup_datetime AS DATE)           AS pickup_date,
    CURRENT_TIMESTAMP                            AS _loaded_at

FROM {{ source("nyc_tlc", "yellow_trips_raw") }}

{% if var('ds') is not none %}
WHERE CAST(tpep_pickup_datetime AS DATE) = DATE '{{ var("ds") }}'
{% endif %}
