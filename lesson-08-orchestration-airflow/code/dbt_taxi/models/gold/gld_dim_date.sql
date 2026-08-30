{{ config(materialized='table') }}

WITH RECURSIVE date_spine(d) AS (
    SELECT DATE '2009-01-01'
    UNION ALL
    SELECT d + INTERVAL '1 day'
    FROM date_spine
    WHERE d < DATE '2030-12-31'
)
SELECT
    CAST(STRFTIME(d, '%Y%m%d') AS INTEGER)          AS date_key,
    d                                                AS full_date,
    EXTRACT(YEAR    FROM d)::SMALLINT                AS year,
    EXTRACT(MONTH   FROM d)::SMALLINT                AS month,
    EXTRACT(DAY     FROM d)::SMALLINT                AS day,
    EXTRACT(DOW     FROM d)::SMALLINT                AS day_of_week,
    DAYNAME(d)                                       AS day_name,
    MONTHNAME(d)                                     AS month_name,
    EXTRACT(QUARTER FROM d)::SMALLINT                AS quarter,
    EXTRACT(DOW FROM d) IN (0, 6)                    AS is_weekend
FROM date_spine
