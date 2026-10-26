-- gold.dim_driver — ЕТАП 2. Grain: водій. SPEC.md, розділ 4.4.
--   * incremental, unique_key='driver_key', delete+insert; за подіями ride_accepted у ref('events')
--   * «останній» рейтинг — за occurred_at; перераховуйте водія з УСІЄЇ його історії, не з батча
--   * плюс член driver_key = 'unknown' (поїздки, скасовані до прийняття)

{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='driver_key'
    )
}}

with accepted as (
    select
        event_id,
        payload #>> '{driver,id}' as driver_key,
        payload #>> '{driver,vehicle,type}' as vehicle_type,
        payload #>> '{driver,vehicle,medallion}' as medallion,
        (payload #>> '{driver,rating}')::numeric as rating,
        occurred_at,
        _ingested_at
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
),

touched_drivers as (
    select distinct driver_key
    from accepted
    where driver_key is not null
    {% if is_incremental() %}
      and _ingested_at > {{ high_watermark() }}
    {% endif %}
),

ranked as (
    select
        a.*,
        row_number() over (
            partition by a.driver_key
            order by a.occurred_at desc, a.event_id desc
        ) as row_num,
        min(a.occurred_at) over (partition by a.driver_key) as first_seen_at,
        max(a.occurred_at) over (partition by a.driver_key) as last_seen_at,
        max(a._ingested_at) over (partition by a.driver_key) as latest_ingested_at
    from accepted as a
    join touched_drivers as t using (driver_key)
),

drivers as (
    select
        driver_key,
        vehicle_type,
        medallion,
        rating as latest_rating,
        first_seen_at,
        last_seen_at,
        latest_ingested_at as _ingested_at,
        '{{ run_started_at }}'::timestamptz as _loaded_at
    from ranked
    where row_num = 1
),

unknown_driver as (
    select
        'unknown'::text as driver_key,
        'Unknown'::text as vehicle_type,
        'Unknown'::text as medallion,
        null::numeric as latest_rating,
        null::timestamptz as first_seen_at,
        null::timestamptz as last_seen_at,
        coalesce(max(_ingested_at), '-infinity'::timestamptz) as _ingested_at,
        '{{ run_started_at }}'::timestamptz as _loaded_at
    from accepted
)

select * from drivers
union all
select * from unknown_driver
