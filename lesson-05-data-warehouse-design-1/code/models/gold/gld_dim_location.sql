{{ config(materialized='table') }}

-- SCD Type 2: each zone version gets its own row and its own surrogate key.
-- For lesson 05 we seed a single version (valid_from = 2024-01-01) for all 265 zones.
-- In lesson 06 a dbt snapshot drives new rows when zone boundaries change.

WITH source AS (
    SELECT
        LocationID   AS location_id,
        Borough      AS borough,
        Zone         AS zone_name,
        service_zone
    FROM {{ ref('seed_taxi_zone') }}
),
versioned AS (
    SELECT
        md5(location_id::VARCHAR || '2024-01-01') AS location_key,
        location_id,
        borough,
        zone_name,
        service_zone,
        DATE '2024-01-01'                         AS valid_from,
        DATE '9999-12-31'                         AS valid_to,
        TRUE                                      AS is_current
    FROM source
)
SELECT * FROM versioned
