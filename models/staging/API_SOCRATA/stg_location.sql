{{
  config(
      materialized='table',
      on_schema_change='fail'
  )
}}

with src_location as (
    select
        address::varchar(100) as address,
        city::varchar(100) as city,
        state::varchar(100) as state,
        case 
            when zipcode is null then null 
            else zipcode::int
        end as zipcode,
        case 
            when latitude is null then null 
            else latitude::float 
        end as latitude,
        case 
            when longitude is null then null 
            else longitude::float 
        end as longitude,
        neighborhood::varchar(100) as neighborhood,
        case 
            when councildistrictcode is null then null 
            else councildistrictcode::int 
        end as council_district_code
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
),

stg_location as (
    select
        case 
            when address is not null and zipcode is not null 
            then {{ dbt_utils.generate_surrogate_key(['address', 'zipcode']) }} 
            else null 
        end as id_location,
        address,
        city,
        state,
        zipcode,
        latitude,
        longitude,
        neighborhood,
        councildistrictcode
    from (
        select distinct 
            address,
            city,
            state,
            zipcode,
            latitude,
            longitude,
            neighborhood,
            councildistrictcode
        from src_location
    ) unique_locations
)

select * from stg_location

select * from stg_location
