-- Transaction fact: one row per taxi trip. All measures additive.
-- Every dimension is JOINed to resolve/conform its key — this both wires the
-- star in lineage and guarantees every FK exists in its dimension:
--   * dim_location (SCD2): join resolves the surrogate location_key.
--   * dim_date / dim_time: smart keys (YYYYMMDD / HHMM) validated against the spine.
--   * lookup dims: unmatched natural keys fold to the -1 "Unknown" member via COALESCE.
--
-- ЩО ЗМІНИЛОСЬ ПРОТИ L05: факт став `incremental` із партицією `pickup_date`.
-- Виміри лишились `table` (перебудовуються цілком — вони маленькі), а факт росте
-- щодня, тож перебудовувати його з нуля на кожному прогоні було б марно.
-- Тест `unique(trip_sk)` у schema.yml — це і є перевірка ідемпотентності:
-- якби повторний прогін дня дописував рядки замість перезапису партиції, він би впав.

{{ config(
    materialized='incremental',
    unique_key='pickup_date',
    incremental_strategy='delete+insert',
) }}

WITH trips AS (
    SELECT
        *,
        CAST(STRFTIME(tpep_pickup_datetime,  '%Y%m%d') AS INTEGER) AS pickup_date_id,
        CAST(STRFTIME(tpep_dropoff_datetime, '%Y%m%d') AS INTEGER) AS dropoff_date_id,
        CAST(STRFTIME(tpep_pickup_datetime,  '%H%M')   AS INTEGER) AS pickup_time_id
    FROM {{ ref('slr_yellow_trips') }}
    {% if var('ds') is not none %}
    WHERE pickup_date = DATE '{{ var("ds") }}'
    {% endif %}
)
SELECT
    md5(
        t.tpep_pickup_datetime::VARCHAR
        || '|' || t.tpep_dropoff_datetime::VARCHAR
        || '|' || t.pu_location_id::VARCHAR
        || '|' || t.do_location_id::VARCHAR
        || '|' || t.fare_amount::VARCHAR
        || '|' || t.total_amount::VARCHAR
    )                                               AS trip_sk,

    -- Date / time keys (role-playing dim_date for pickup + dropoff)
    ddp.date_key                                    AS pickup_date_key,
    ddd.date_key                                    AS dropoff_date_key,
    dt.time_key                                     AS pickup_time_key,

    -- Location keys (role-playing dim_location, SCD2 surrogate)
    pu.location_key                                 AS pu_location_key,
    do_.location_key                                AS do_location_key,

    -- Lookup dim keys (unmatched → -1 Unknown member)
    COALESCE(dv.vendor_id,       -1)                AS vendor_key,
    COALESCE(dr.rate_code_id,    -1)                AS rate_code_key,
    COALESCE(dp.payment_type_id, -1)                AS payment_type_key,

    -- Additive measures
    t.fare_amount,
    t.tip_amount,
    t.tolls_amount,
    t.total_amount,
    t.trip_distance,
    DATEDIFF('second', t.tpep_pickup_datetime, t.tpep_dropoff_datetime)::BIGINT AS duration_sec,
    t.passenger_count,

    -- поля партиціонування та аудиту
    t.pickup_date,
    CURRENT_TIMESTAMP                               AS _loaded_at

FROM trips t
LEFT JOIN {{ ref('gld_dim_location') }} pu
    ON  t.pu_location_id = pu.location_id AND pu.is_current
LEFT JOIN {{ ref('gld_dim_location') }} do_
    ON  t.do_location_id = do_.location_id AND do_.is_current
LEFT JOIN {{ ref('gld_dim_date') }} ddp ON ddp.date_key = t.pickup_date_id
LEFT JOIN {{ ref('gld_dim_date') }} ddd ON ddd.date_key = t.dropoff_date_id
LEFT JOIN {{ ref('gld_dim_time') }} dt  ON dt.time_key  = t.pickup_time_id
LEFT JOIN {{ ref('gld_dim_vendor') }} dv       ON dv.vendor_id       = COALESCE(t.vendor_id,   -1)
LEFT JOIN {{ ref('gld_dim_rate_code') }} dr    ON dr.rate_code_id    = COALESCE(t.ratecode_id, -1)
LEFT JOIN {{ ref('gld_dim_payment_type') }} dp ON dp.payment_type_id = COALESCE(t.payment_type, -1)
