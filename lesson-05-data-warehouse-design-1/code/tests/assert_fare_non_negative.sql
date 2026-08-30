-- Singular test: stg_yellow_trips must contain no negative fares.
-- stg filters fare_amount >= 0, so this query must return 0 rows to pass.
SELECT *
FROM {{ ref("slr_yellow_trips") }}
WHERE fare_amount < 0
