{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select *
    from {{ source('beejanride_raw', 'pg_payments_raw') }}
    where payment_id is not null                

    {% if is_incremental() %}
        and created_at > (select max(created_at) from {{ this }})
    {% endif %}

),

cleaned as (

    select
        payment_id,
        trip_id,                                  
        lower(trim(payment_status)) as payment_status,
        lower(trim(payment_provider)) as payment_provider,

        cast(amount as numeric) as amount,
        cast(fee as numeric)  as fee,
        upper(trim(currency)) as currency,         

        {{ standardize_timestamp('created_at') }} as created_at


    from source

),

deduplicated as (

    {{ deduplicate('cleaned', 'payment_id', 'created_at') }}

)

select * from deduplicated