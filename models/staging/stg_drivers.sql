with source as (

    select *
    from {{ source('beejanride_raw', 'pg_drivers_raw') }}
    where driver_id is not null

),

cleaned as (

    select
        driver_id,
        onboarding_date,
        
        lower(trim(driver_status)) as driver_status,
        city_id,
        vehicle_id,
        
        cast(rating as float64) as rating,           
        {{ standardize_timestamp('created_at') }} as created_at,
        {{ standardize_timestamp('updated_at') }} as updated_at

    from source

),

deduplicated as (

    {{ deduplicate('cleaned', 'driver_id','updated_at' )}}

)

select * from deduplicated