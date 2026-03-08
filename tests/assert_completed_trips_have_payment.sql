-- This test finds completed trips where no successful payment exists.
-- Every completed trip should have been paid for.
-- If this returns rows, it means revenue was lost or data is inconsistent.
-- Note: these trips are also flagged in int_trips_flagged as
-- is_completed_with_failed_payment = true

select
    trip_id,
    trip_status,
    payment_status,
    payment_availability
from {{ ref('fct_trips') }}
where trip_status = 'completed'
  and payment_status != 'success'