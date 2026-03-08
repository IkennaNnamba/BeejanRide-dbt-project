{% snapshot drivers_snapshot %}

{{
    config(
        target_schema='BeejanRide_snapshots',
        unique_key='driver_id',
        strategy='timestamp',
        updated_at='updated_at'
    )
}}

select *
from {{ ref('stg_drivers') }}

{% endsnapshot %}

