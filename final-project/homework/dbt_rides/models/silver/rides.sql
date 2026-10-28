-- silver.rides — ЕТАП 2. Grain: одна поїздка (ride_id). SPEC.md, розділ 4.3.
--   * incremental, unique_key='ride_id', delete+insert
--   * перебудовуйте поїздки, яких торкнулися НОВІ події, з УСІЄЇ їхньої історії в ref('events')
--   * поїздка існує, коли прийшла її ride_requested; порядок прибуття подій байдужий
--   * status, фактична зона (перекриває заявлену), wait_seconds, trip_seconds, суми з ride_completed
--   * _ingested_at = максимум _ingested_at усіх подій поїздки

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='ride_id'
    )
}}

with touched_rides as (
    select distinct ride_id
    from {{ ref('events') }}
    where 1 = 1
    {% if is_incremental() %}
      and _ingested_at > {{ high_watermark() }}
    {% endif %}
),

history as (
    select e.*
    from {{ ref('events') }} as e
    join touched_rides as t using (ride_id)
),

pivoted as (
    select
        ride_id,
        max(payload #>> '{rider,id}')
            filter (where event_type = 'ride_requested') as rider_id,
        max(payload #>> '{rider,platform}')
            filter (where event_type = 'ride_requested') as rider_platform,
        max(payload #>> '{rider,app_version}')
            filter (where event_type = 'ride_requested') as app_version,
        max(payload ->> 'requested_vehicle')
            filter (where event_type = 'ride_requested') as requested_vehicle,

        coalesce(
            max(payload #>> '{driver,id}') filter (where event_type = 'ride_completed'),
            max(payload #>> '{driver,id}') filter (where event_type = 'ride_started'),
            max(payload #>> '{driver,id}') filter (where event_type = 'ride_accepted')
        ) as driver_id,

        min(occurred_at) filter (where event_type = 'ride_requested') as requested_at,
        min(occurred_at) filter (where event_type = 'ride_accepted') as accepted_at,
        min(occurred_at) filter (where event_type = 'ride_started') as started_at,
        min(occurred_at) filter (where event_type = 'ride_completed') as completed_at,
        min(occurred_at) filter (where event_type = 'ride_cancelled') as cancelled_at,
        min(occurred_at) filter (where event_type = 'payment_captured') as paid_at,

        coalesce(
            max((payload #>> '{pickup,zone_id}')::int)
                filter (where event_type = 'ride_started'),
            max((payload #>> '{pickup,zone_id}')::int)
                filter (where event_type = 'ride_requested')
        ) as pickup_zone_id,
        coalesce(
            max((payload #>> '{dropoff,zone_id}')::int)
                filter (where event_type = 'ride_completed'),
            max((payload #>> '{dropoff,zone_id}')::int)
                filter (where event_type = 'ride_requested')
        ) as dropoff_zone_id,

        max((payload ->> 'surge_estimate')::numeric)
            filter (where event_type = 'ride_requested') as surge_estimate,
        max((payload ->> 'distance_km')::numeric(8, 2))
            filter (where event_type = 'ride_completed') as distance_km,
        max((payload #>> '{fare,surge_multiplier}')::numeric(4, 2))
            filter (where event_type = 'ride_completed') as surge_multiplier,
        max((payload #>> '{fare,amount}')::numeric(10, 2))
            filter (where event_type = 'ride_completed') as fare_amount,
        max((payload #>> '{fare,tolls}')::numeric(10, 2))
            filter (where event_type = 'ride_completed') as tolls_amount,
        max((payload #>> '{fare,tip}')::numeric(10, 2))
            filter (where event_type = 'ride_completed') as tip_amount,
        max((payload #>> '{fare,total}')::numeric(10, 2))
            filter (where event_type = 'ride_completed') as total_amount,
        max(payload #>> '{fare,currency}')
            filter (where event_type = 'ride_completed') as currency,

        max(payload ->> 'cancelled_by')
            filter (where event_type = 'ride_cancelled') as cancelled_by,
        max(payload ->> 'reason')
            filter (where event_type = 'ride_cancelled') as cancel_reason,
        max(payload ->> 'stage')
            filter (where event_type = 'ride_cancelled') as cancel_stage,

        max(payload #>> '{payment,method}')
            filter (where event_type = 'payment_captured') as payment_method,
        max(payload #>> '{payment,psp_reference}')
            filter (where event_type = 'payment_captured') as psp_reference,
        max((payload #>> '{payment,amount}')::numeric(10, 2))
            filter (where event_type = 'payment_captured') as payment_amount,
        max(_ingested_at) as _ingested_at
    from history
    group by ride_id
    having count(*) filter (where event_type = 'ride_requested') > 0
)

select
    ride_id,
    rider_id,
    rider_platform,
    app_version,
    requested_vehicle,
    driver_id,
    case
        when completed_at is not null then 'completed'
        when cancelled_at is not null then 'cancelled'
        when started_at is not null then 'in_progress'
        when accepted_at is not null then 'accepted'
        else 'requested'
    end as status,
    requested_at,
    accepted_at,
    started_at,
    completed_at,
    cancelled_at,
    paid_at,
    pickup_zone_id,
    dropoff_zone_id,
    extract(epoch from accepted_at - requested_at)::int as wait_seconds,
    extract(epoch from completed_at - started_at)::int as trip_seconds,
    surge_estimate,
    distance_km,
    surge_multiplier,
    fare_amount,
    tolls_amount,
    tip_amount,
    total_amount,
    currency,
    cancelled_by,
    cancel_reason,
    cancel_stage,
    payment_method,
    psp_reference,
    payment_amount,
    _ingested_at,
    '{{ run_started_at }}'::timestamptz as _loaded_at
from pivoted
