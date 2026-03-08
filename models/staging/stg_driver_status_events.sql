{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select *
    from {{ source('beejanride_raw', 'pg_driver_status_events_raw') }}
    where event_id is not null               

    {% if is_incremental() %}
        and event_timestamp > (select max(event_timestamp) from {{ this }})
    {% endif %}
),

cleaned as (

    select
        event_id,
        driver_id,                            

        lower(trim(status)) as status,          

        {{ standardize_timestamp('event_timestamp') }} as event_timestamp

    from source

),

deduplicated as (

    {{ deduplicate('cleaned', 'event_id', 'event_timestamp') }}

)

select * from deduplicated