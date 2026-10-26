{{ config(tags=['reconcile']) }}

-- Driver attributes must come from the latest accepted event by event time, even when an
-- older accepted event arrives in a later ingestion batch.
with ranked as (
    select
        payload #>> '{driver,id}' as driver_key,
        payload #>> '{driver,vehicle,type}' as vehicle_type,
        payload #>> '{driver,vehicle,medallion}' as medallion,
        (payload #>> '{driver,rating}')::numeric as latest_rating,
        row_number() over (
            partition by payload #>> '{driver,id}'
            order by occurred_at desc, event_id desc
        ) as row_num
    from {{ ref('events') }}
    where event_type = 'ride_accepted'
),
expected as (
    select driver_key, vehicle_type, medallion, latest_rating
    from ranked
    where row_num = 1
)
select coalesce(d.driver_key, e.driver_key) as driver_key
from {{ ref('dim_driver') }} as d
full outer join expected as e using (driver_key)
where coalesce(d.driver_key, e.driver_key) <> 'unknown'
  and (
      d.driver_key is null
      or e.driver_key is null
      or d.vehicle_type is distinct from e.vehicle_type
      or d.medallion is distinct from e.medallion
      or d.latest_rating is distinct from e.latest_rating
  )
