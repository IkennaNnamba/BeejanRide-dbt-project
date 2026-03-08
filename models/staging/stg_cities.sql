with source as (

    select *
    from {{ source('beejanride_raw', 'pg_cities_raw') }}
    where city_id is not null              

),

cleaned as (

    select
        city_id,
        lower(trim(city_name))  as city_name, 
        lower(trim(country))    as country,
        
        {{ standardize_timestamp('launch_date') }} as launch_date,   

    from source

),

deduplicated as (

    {{ deduplicate('cleaned', 'city_id', 'launch_date') }}

)

select * from deduplicated
