with trips as (

    select * from {{ ref('int_trips_enriched') }}

),

summary as (

    select
        driver_id,

        count(trip_id)                                          as total_trips,
        countif(status = 'completed')                          as completed_trips,
        countif(status = 'cancelled')                          as cancelled_trips,
        countif(status = 'no_show')                            as no_show_trips,

        -- revenue (only count completed trips)
        round(
            sum(case when status = 'completed'
                then actual_fare else 0 end), 2
        )                                           as total_revenue,

        round(avg(trip_duration_minutes), 2)        as avg_trip_duration_minutes,
        date(min(requested_at))                     as first_trip_date,
        date(max(requested_at))                     as last_trip_date,

        -- how many days since their last trip
        date_diff(current_date(), date(max(requested_at)), day) as days_since_last_trip

    from trips
    group by driver_id

),

with_churn_flag as (

    select
        *,
        -- churn flag: driver hasn't completed a trip in 30+ days
        case
            when days_since_last_trip > 30 then true
            else false
        end                                                     as is_churned,
        -- completion rate: what percentage of their trips did they finish
        round(
            safe_divide(completed_trips, total_trips) * 100, 2
        )                                                       as completion_rate_pct

    from summary

)

select * from with_churn_flag