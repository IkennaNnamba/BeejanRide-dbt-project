--{#
--    INCREMENTAL CONFIG — uncomment when BigQuery billing is enabled:
--    materialized='incremental',
--    unique_key='trip_id',
--    on_schema_change='sync_all_columns',
--#}

{{
    config(
        materialized='table',
        description='Core fact table. One row per trip with enriched metrics, revenue, and fraud signals. Primary table for dashboards and analysis.'
    )
}}

with trips as (

    select * from {{ ref('int_trips_enriched') }}

    --{INCREMENTAL FILTER — uncomment when BigQuery billing is enabled:
    --   {% if is_incremental() %}
    --       and updated_at > (select max(updated_at) from {{ this }})
    --   {% endif %}
    --}

),

revenue as (

    select
        trip_id,
        gross_revenue,
        net_revenue,
        payment_fee,
        payment_status,
        payment_provider,
        payment_availability,
        revenue_type,
        is_revenue_realised,
        currency
    from {{ ref('int_trips_revenue') }}

),

fraud as (

    select
        trip_id,
        is_extreme_surge,
        is_completed_with_failed_payment,
        has_duplicate_payment,
        is_suspicious_duration,
        is_zero_fare_completed,
        fraud_signal_score,
        risk_tier
    from {{ ref('int_trips_flagged') }}

),

final as (

    select
        -- primary key
        t.trip_id,

        -- foreign keys for dimension joins
        t.rider_id,
        t.driver_id,
        t.city_id,

        -- date key for time-based analysis
        t.requested_at           as trip_date,

        -- trip details
        t.status                        as trip_status,
        t.payment_method,
        t.is_corporate,
        t.surge_multiplier,
        t.is_surge,

        -- timestamps
        t.requested_at,
        t.pickup_at,
        t.dropoff_at,

        -- measures
        t.trip_duration_minutes,
        t.estimated_fare,
        t.actual_fare,
        t.base_fare,
        t.surge_revenue_impact,

        -- revenue measures
        r.gross_revenue,
        r.net_revenue,
        r.payment_fee,
        r.revenue_type,
        r.payment_status,
        r.payment_provider,
        r.payment_availability,
        r.is_revenue_realised,
        r.currency,

        -- fraud signals
        f.is_extreme_surge,
        f.is_completed_with_failed_payment,
        f.has_duplicate_payment,
        f.is_suspicious_duration,
        f.is_zero_fare_completed,
        f.fraud_signal_score,
        f.risk_tier,

        -- metadata
        t.created_at,
        t.updated_at

    from trips t
    left join revenue r on t.trip_id = r.trip_id
    left join fraud   f on t.trip_id = f.trip_id

)

select * from final