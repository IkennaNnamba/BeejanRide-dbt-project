{{
    config(
        materialized='incremental',
        unique_key='rider_id',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select *
    from {{ source('beejanride_raw', 'pg_riders_raw') }}
    where rider_id is not null                   -- early filter invalid PKs

    {% if is_incremental() %}
        and created_at > (select max(created_at) from {{ this }})
    {% endif %}
    
),

cleaned as (

    select
        rider_id,
        
        {{ standardize_timestamp('signup_date') }}   as signup_date,
        
        lower(trim(country))                         as country,
        referral_code,                               

        {{ standardize_timestamp('created_at') }}    as created_at

    from source

),

deduplicated as (

    {{ deduplicate('cleaned', 'rider_id', 'created_at') }}

)

select * from deduplicated