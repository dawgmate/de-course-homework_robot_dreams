-- Silver — очищені поїздки (ті самі DQ-фільтри, що в L05).
--
-- ЩО ЗМІНИЛОСЬ ПРОТИ L05: був `view` над усім bronze, став `incremental` з тією
-- самою партицією `pickup_date`. Причина суто оркестраційна: кожен прогін DAG-у
-- обробляє один день, і кожен шар має вміти переписати рівно свою партицію.

{{ config(
    materialized="incremental",
    unique_key="pickup_date",
    incremental_strategy="delete+insert",
) }}

SELECT
    * EXCLUDE (_loaded_at),
    CURRENT_TIMESTAMP AS _loaded_at
FROM {{ ref("brz_yellow_trips") }}
WHERE fare_amount           >= 0
  AND trip_distance         >= 0
  AND tpep_pickup_datetime  IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  AND pu_location_id        IS NOT NULL
  AND do_location_id        IS NOT NULL

{% if var('ds') is not none %}
  AND pickup_date = DATE '{{ var("ds") }}'
{% endif %}
