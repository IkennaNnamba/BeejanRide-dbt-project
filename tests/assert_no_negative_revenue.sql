-- This test finds any completed trips where net_revenue is negative.
-- A negative net_revenue means BeejanRide lost money on a trip
-- which should never happen in normal operations.
-- If this test returns any rows, something is wrong with fare or fee data.

select
    trip_id,
    net_revenue
from {{ ref('fct_trips') }}
where is_revenue_realised = true
  and net_revenue < 0