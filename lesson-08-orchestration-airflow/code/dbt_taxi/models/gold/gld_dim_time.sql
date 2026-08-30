{{ config(materialized='table') }}

-- NB: DuckDB `/` is float division; use `//` for integer division.
WITH RECURSIVE minutes(n) AS (
    SELECT 0
    UNION ALL
    SELECT n + 1
    FROM minutes
    WHERE n < 1439
)
SELECT
    ((n // 60) * 100 + (n % 60))::INTEGER           AS time_key,
    (n // 60)::SMALLINT                             AS hour,
    (n % 60)::SMALLINT                              AS minute,
    (n // 60) BETWEEN 7 AND 9
        OR (n // 60) BETWEEN 16 AND 18             AS is_rush_hour,
    CASE
        WHEN (n // 60) <  6 THEN 'Night'
        WHEN (n // 60) < 12 THEN 'Morning'
        WHEN (n // 60) < 18 THEN 'Afternoon'
        ELSE 'Evening'
    END                                             AS time_of_day
FROM minutes
