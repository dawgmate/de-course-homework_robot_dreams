{{ config(materialized='table') }}

-- SCD Type 1 lookup. Domain maintained as a seed (seed_rate_code.csv).
SELECT
    CAST(rate_code_id AS INTEGER) AS rate_code_id,
    rate_code_description
FROM {{ ref('seed_rate_code') }}
