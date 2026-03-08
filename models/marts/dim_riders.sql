select
    r.rider_id,
    r.signup_date,
    r.country,
    r.referral_code,

    -- lifetime value metrics from intermediate
    ltv.total_trips,
    ltv.completed_trips,
    ltv.total_spent,
    ltv.avg_fare_per_trip,
    ltv.first_trip_date,
    ltv.last_trip_date,
    ltv.days_since_last_trip,
    ltv.is_active,
    ltv.days_to_first_trip

from {{ ref('stg_riders') }} r
left join {{ ref('int_rider_lifetime_value') }} ltv
    on r.rider_id = ltv.rider_id