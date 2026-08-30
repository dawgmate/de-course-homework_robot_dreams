{{ config(materialized='table') }}

-- SCD Type 1 lookup. Domain maintained as a seed (seed_payment_type.csv).
SELECT
    CAST(payment_type_id AS INTEGER) AS payment_type_id,
    payment_type_description
FROM {{ ref('seed_payment_type') }}
