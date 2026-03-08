-- This test finds completed trips where trip duration is zero or negative.
-- A zero or negative duration means dropoff_at <= pickup_at
-- which is physically impossible and indicates bad timestamp data.

select
    trip_id,
    trip_duration_minutes,
    pickup_at,
    dropoff_at
from {{ ref('fct_trips') }}
where trip_status = 'completed'
  and trip_duration_minutes is not null
  and trip_duration_minutes <= 0