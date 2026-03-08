with drivers as (

    select * from {{ ref('stg_drivers') }}

),

trip_summary as (

    select * from {{ ref('int_driver_trip_summary') }}

),

activity as (

    select * from {{ ref('int_driver_activity') }}

),

final as (

    select
        -- driver profile
        d.driver_id,
        d.onboarding_date,
        d.driver_status,
        d.city_id,
        d.vehicle_id,
        d.rating,

        -- trip performance
        t.total_trips,
        t.completed_trips,
        t.cancelled_trips,
        t.no_show_trips,
        t.total_revenue,
        t.avg_trip_duration_minutes,
        t.completion_rate_pct,
        t.first_trip_date,
        t.last_trip_date,
        t.days_since_last_trip,
        t.is_churned,

        -- online activity
        a.total_online_hours,
        a.total_online_sessions,
        a.avg_session_duration_minutes,
        a.first_online_date,
        a.last_online_date

    from drivers d
    left join trip_summary t on d.driver_id = t.driver_id
    left join activity     a on d.driver_id = a.driver_id

)

select * from final