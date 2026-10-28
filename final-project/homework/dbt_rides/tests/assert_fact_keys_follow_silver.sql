{{ config(tags=['reconcile']) }}

-- Known natural keys in Silver must map to the same Gold keys; only missing values may use
-- the unknown members.
select f.ride_id
from {{ ref('fact_ride') }} as f
join {{ ref('rides') }} as r using (ride_id)
where f.pickup_zone_key is distinct from coalesce(r.pickup_zone_id, -1)
   or f.dropoff_zone_key is distinct from coalesce(r.dropoff_zone_id, -1)
   or f.driver_key is distinct from coalesce(r.driver_id, 'unknown')
