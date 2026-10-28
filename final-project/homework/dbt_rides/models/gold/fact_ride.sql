-- gold.fact_ride — ЕТАП 2. Grain: поїздка (accumulating snapshot). SPEC.md, розділ 4.4.
--   * incremental, unique_key='ride_id', delete+insert; беріть з ref('rides') те, що змінилося
--   * FK до вимірів через LEFT JOIN + COALESCE (-1 / 'unknown'): рядок не губиться
--   * requested_date = utc_date(requested_at); requested_hour = початок години за UTC

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='ride_id'
    )
}}

with changed_rides as (
    select *
    from {{ ref('rides') }}
    where 1 = 1
    {% if is_incremental() %}
      and _ingested_at > {{ high_watermark() }}
    {% endif %}
)

select
    r.ride_id,
    coalesce(pu.zone_key, -1) as pickup_zone_key,
    coalesce(doz.zone_key, -1) as dropoff_zone_key,
    coalesce(d.driver_key, 'unknown') as driver_key,
    r.rider_id,
    r.status,
    r.requested_at,
    r.accepted_at,
    r.started_at,
    r.completed_at,
    r.cancelled_at,
    r.paid_at,
    {{ utc_date('r.requested_at') }} as requested_date,
    date_trunc('hour', r.requested_at at time zone 'UTC') at time zone 'UTC'
        as requested_hour,
    r.wait_seconds,
    r.trip_seconds,
    r.distance_km,
    r.fare_amount,
    r.surge_multiplier,
    r.tolls_amount,
    r.tip_amount,
    r.total_amount,
    r.currency,
    r.payment_method,
    r.payment_amount,
    r.cancelled_by,
    r.cancel_reason,
    r.cancel_stage,
    r._ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from changed_rides as r
left join {{ ref('dim_zone') }} as pu
    on pu.zone_key = r.pickup_zone_id
left join {{ ref('dim_zone') }} as doz
    on doz.zone_key = r.dropoff_zone_id
left join {{ ref('dim_driver') }} as d
    on d.driver_key = r.driver_id
