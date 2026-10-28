-- gold.agg_zone_hourly — ЕТАП 2. Grain: (requested_hour, pickup_zone_key). SPEC.md, розділ 4.4.
--   * incremental; після будь-якого інкременту = перерахунок із fact_ride рядок у рядок
--   * подумайте: що перераховувати, коли поїздка змінила зону? Який ключ стабільний?

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='requested_hour'
    )
}}

with affected_hours as (
    select distinct requested_hour
    from {{ ref('fact_ride') }}
    where 1 = 1
    {% if is_incremental() %}
      and _ingested_at > {{ high_watermark() }}
    {% endif %}
),

rides_in_affected_hours as (
    select f.*
    from {{ ref('fact_ride') }} as f
    join affected_hours as h using (requested_hour)
)

select
    requested_hour,
    pickup_zone_key,
    count(*) as rides_requested,
    count(*) filter (where status = 'completed') as rides_completed,
    count(*) filter (where status = 'cancelled') as rides_cancelled,
    coalesce(
        sum(total_amount) filter (where status = 'completed'),
        0
    )::numeric(12, 2) as gross_revenue,
    coalesce(
        sum(tip_amount) filter (where status = 'completed'),
        0
    )::numeric(12, 2) as tips,
    avg(wait_seconds)::numeric(12, 2) as avg_wait_seconds,
    max(_ingested_at) as _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from rides_in_affected_hours
group by requested_hour, pickup_zone_key
