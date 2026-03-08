-- {#
--    INCREMENTAL CONFIG — uncomment when BigQuery billing is enabled:
--    materialized='incremental',
--    unique_key='trip_id',
--    on_schema_change='sync_all_columns',
--#}

{{
    config(
        materialized='table',
        description='Core enriched trips spine...'
    )
}}

with trips as (

    select * from {{ ref('stg_trips') }}

    -- INCREMENTAL FILTER (uncomment when BigQuery billing is enabled)
    -- {% if is_incremental() %}
    --     and updated_at > (select max(updated_at) from {{ this }})
    -- {% endif %}

),

cities as (

    select
        city_id,
        city_name,
        country as city_country
    from {{ ref('stg_cities') }}

),

enriched as (

    select

        t.trip_id,
        t.rider_id,
        t.driver_id,
        t.vehicle_id,
        t.city_id,

        c.city_name,
        c.city_country,

        t.requested_at,
        t.pickup_at,
        t.dropoff_at,
        t.created_at,
        t.updated_at,

        t.status,
        t.payment_method,
        t.is_corporate,

        -- trip duration
        -- null for non-completed trips or missing timestamps
        case
            when t.status = 'completed'
                and t.pickup_at  is not null
                and t.dropoff_at is not null
                and t.dropoff_at > t.pickup_at 
            then timestamp_diff(t.dropoff_at, t.pickup_at, minute)
            else null
        end as trip_duration_minutes,


        t.estimated_fare,
        t.actual_fare,
        t.surge_multiplier,

        t.surge_multiplier > 1.0 as is_surge,

        -- base_fare: what the fare would have been without surge
        -- safe_divide protects against surge_multiplier being 0
        round(
            safe_divide(t.actual_fare, t.surge_multiplier), 2
        ) as base_fare,

        -- surge_revenue_impact: extra money surge generated on this trip
        round(
            t.actual_fare - safe_divide(t.actual_fare, t.surge_multiplier), 2
        ) as surge_revenue_impact

    from trips t
    left join cities c on t.city_id = c.city_id

)

select * from enriched