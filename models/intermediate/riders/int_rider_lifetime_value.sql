with trips as (

    select * from {{ ref('int_trips_enriched') }}

),

riders as (

    select * from {{ ref('stg_riders') }}

),

trip_summary as (

    select
        rider_id,

        -- trip counts
        count(trip_id)                                          as total_trips,
        countif(status = 'completed')                          as completed_trips,
        countif(status = 'cancelled')                          as cancelled_trips,

        -- spending
        round(
            sum(case when status = 'completed'
                then actual_fare else 0 end), 2
        )                                                       as total_spent,

        round(
            avg(case when status = 'completed'
                then actual_fare end), 2
        )                                                       as avg_fare_per_trip,

        -- activity dates
        date(min(requested_at))                                as first_trip_date,
        date(max(requested_at))                                as last_trip_date,

        -- days since last trip
        date_diff(current_date(), date(max(requested_at)), day) as days_since_last_trip

    from trips
    group by rider_id

),

-- join back to riders to include signup info
final as (

    select
        r.rider_id,
        r.signup_date,
        r.country,
        r.referral_code,
        t.total_trips,
        t.completed_trips,
        t.cancelled_trips,
        t.total_spent,
        t.avg_fare_per_trip,
        t.first_trip_date,
        t.last_trip_date,
        t.days_since_last_trip,

        -- is the rider still active?
        case
            when t.days_since_last_trip <= 30 then true
            else false
        end                                                     as is_active,

        -- how many days between signup and first trip
        -- tells you how quickly riders convert after signing up
        date_diff(t.first_trip_date, date(r.signup_date), day) as days_to_first_trip

    from riders r
    left join trip_summary t on r.rider_id = t.rider_id

)

select * from final