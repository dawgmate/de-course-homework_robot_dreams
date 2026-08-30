{{ config(materialized='table') }}

-- SCD Type 1 lookup. Domain maintained as a seed (seed_vendor.csv);
-- this model just types it and exposes it as the gold dimension.
SELECT
    CAST(vendor_id AS INTEGER) AS vendor_id,
    vendor_name
FROM {{ ref('seed_vendor') }}
