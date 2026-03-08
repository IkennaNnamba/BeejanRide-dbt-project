with trips as (

    select * from {{ ref('int_trips_enriched') }}

),

payments as (

    select
        trip_id,
        max(payment_status)                        as payment_status,
        count(*)                                   as payment_count

    from {{ ref('stg_payments') }}
    group by trip_id 
),

thresholds as (

    select * from {{ ref('int_surge_thresholds') }}

),

flagged as (

    select
        t.trip_id,
        t.rider_id,
        t.driver_id,
        t.city_id,
        t.city_name,
        t.status,
        t.actual_fare,
        t.surge_multiplier,
        t.trip_duration_minutes,
        t.requested_at,
        p.payment_status,

        -- flag 1: surge is way above normal
        case
            when t.surge_multiplier > 10.0
                or t.surge_multiplier > s.p99_surge
            then true
            else false
        end as is_extreme_surge,

        -- flag 2: trip completed but payment failed
        case
            when t.status = 'completed'
                and p.payment_status = 'failed'
            then true
            else false
        end as is_completed_with_failed_payment,

        -- flag 3: same trip was paid more than once
        case
            when p.payment_count > 1
            then true
            else false
        end as has_duplicate_payment,

        -- flag 4: trip duration is too short or too long
        case
            when t.status = 'completed'
                and (
                    t.trip_duration_minutes < 2
                    or t.trip_duration_minutes > 480
                )
            then true
            else false
        end as is_suspicious_duration,

        -- flag 5: trip completed but fare was zero
        case
            when t.status = 'completed'
                and coalesce(t.actual_fare, 0) = 0
            then true
            else false
        end as is_zero_fare_completed

    from trips t
    left join payments p on t.trip_id = p.trip_id
    cross join thresholds s

),

scored as (

    select
        *,

        -- add up all the flags into one score
        cast(is_extreme_surge as int64)
        + cast(is_completed_with_failed_payment as int64)
        + cast(has_duplicate_payment as int64)
        + cast(is_suspicious_duration as int64)
        + cast(is_zero_fare_completed as int64)  as fraud_signal_score

    from flagged

),

-- translate the score into a risk level
final as (

    select
        *,
        case
            when fraud_signal_score = 0 then 'clean'
            when fraud_signal_score = 1 then 'low'
            when fraud_signal_score = 2 then 'medium'
            else 'high'
        end as risk_tier

    from scored

)

select * from final