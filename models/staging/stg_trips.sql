{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select *
    from {{ source('beejanride_raw', 'pg_trips_raw') }}
    where trip_id is not null
    
    {% if is_incremental() %}
        and updated_at > (select max(updated_at) from {{ this }})
    {% endif %}

),


cleaned as (

    select
        trip_id,
        rider_id,
        driver_id,
        vehicle_id,
        city_id,

        {{ standardize_timestamp('requested_at') }} as requested_at,
        {{ standardize_timestamp('pickup_at') }} as pickup_at,
        {{ standardize_timestamp('dropoff_at') }} as dropoff_at,

        lower(status) as status,

        estimated_fare,
        actual_fare,
        
        cast(surge_multiplier as float64) as surge_multiplier,

        lower(payment_method) as payment_method,
        
        cast(is_corporate as boolean) as is_corporate,

        {{ standardize_timestamp('created_at') }} as created_at,
        {{ standardize_timestamp('updated_at') }} as updated_at

    from source

),

deduplicated as (

    {{ deduplicate('cleaned', 'trip_id', 'updated_at') }}

)

select * from deduplicated

