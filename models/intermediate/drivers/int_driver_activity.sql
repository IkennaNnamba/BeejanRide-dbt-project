with events as (

    select * from {{ ref('stg_driver_status_events') }}

),

-- pair each online event with the next event timestamp
paired_events as (

    select
        driver_id,
        status,
        event_timestamp as online_at,

        -- get the timestamp of the very next event for this driver
        lead(event_timestamp) over (
            partition by driver_id
            order by event_timestamp
        ) as next_event_at

    from events

),

-- keep only online events that have a matching next timestamp
-- this gives us clean online → offline pairs
online_sessions as (

    select
        driver_id,
        online_at,
        next_event_at                                       as offline_at,

        -- calculate how long this session lasted in minutes
        timestamp_diff(next_event_at, online_at, minute)   as session_duration_minutes

    from paired_events

    -- only online events with a valid next timestamp
    where status = 'online'
      and next_event_at is not null

      -- guard: next event must be after online event
      -- protects against bad data
      and next_event_at > online_at

),

-- summarise to one row per driver
summary as (

    select
        driver_id,

        -- total online time
        round(
            sum(session_duration_minutes) / 60, 2
        )                                                   as total_online_hours,
        -- number of times they came online
        count(*)                                            as total_online_sessions,

        round(
            avg(session_duration_minutes), 2
        )                                                   as avg_session_duration_minutes,

        date(min(online_at))                                as first_online_date,
        date(max(online_at))                                as last_online_date

    from online_sessions
    group by driver_id

)

select * from summary