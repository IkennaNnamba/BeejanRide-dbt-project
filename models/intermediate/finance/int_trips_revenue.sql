--{#
--    INCREMENTAL CONFIG — uncomment when BigQuery billing is enabled:
--    materialized='incremental',
--    unique_key='trip_id',
--    on_schema_change='sync_all_columns',
--#}

{{
    config(
        materialized='table',
        description='Trip-level revenue model. Calculates gross revenue, net revenue, payment fees, and revenue type per trip. Builds on int_trips_enriched spine.'
    )
}}

with trips as (

    select * from {{ ref('int_trips_enriched') }}
    
    --{ INCREMENTAL FILTER — uncomment when BigQuery billing is enabled:
    --   {% if is_incremental() %}
    --       and updated_at > (select max(updated_at) from {{ this }})
    --   {% endif %}
    --}

),

payments as (

    select
        trip_id,
        max(payment_id)       as payment_id,
        max(payment_status)   as payment_status,
        max(payment_provider) as payment_provider,
        max(amount)           as payment_amount,
        max(fee)              as payment_fee,
        max(currency)         as currency
    from {{ ref('stg_payments') }}
    group by trip_id

),

revenue as (

    select
        t.trip_id,
        t.rider_id,
        t.driver_id,
        t.city_id,
        t.city_name,
        t.city_country,

        t.status                    as trip_status,
        t.is_corporate,
        t.payment_method,
        t.requested_at,
        t.surge_multiplier,
        t.is_surge,

        -- makes corporate vs personal splits trivial in marts
        case
            when t.is_corporate = true then 'corporate'
            else 'personal'
        end as revenue_type,

        p.payment_id,
        p.payment_status,
        p.payment_provider,
        p.currency,

        -- payment availability flag
        -- helps distinguish "no payment record" from "failed payment"
        case
            when p.payment_id is null then 'no_payment_record'
            when p.payment_status = 'failed' then 'payment_failed'
            when p.payment_status = 'success' then 'payment_success'
        end as payment_availability,

        t.actual_fare as gross_revenue,

        -- coalesce to 0 so net_revenue is never null on completed trips
        coalesce(p.payment_fee, 0) as payment_fee,

        -- net revenue
        -- what BeejanRide actually keeps after paying the payment provider
        -- only meaningful when payment was successful
        case
            when p.payment_status = 'success'
            then round(t.actual_fare - coalesce(p.payment_fee, 0), 2)
            else 0
        end as net_revenue,

        -- realised revenue flag
        -- true only when trip completed AND payment succeeded
        -- this is the only revenue BeejanRide has actually collected
        case
            when t.status = 'completed'
                and p.payment_status = 'success'
            then true
            else false
        end as is_revenue_realised,

        -- surge revenue isolated
        t.surge_revenue_impact,
        t.base_fare

    from trips t
    left join payments p on t.trip_id = p.trip_id

)

select * from revenue