{{
  config(
      materialized='table',
      on_schema_change='fail'
  )
}}

with src_location as (
    select
        case 
            when address is null or trim(address) = '' then null 
            else address
        end as address::varchar(100),
        case 
            when city is null or trim(city) = '' then null 
            else city
        end as city::varchar(100),
        case 
            when state is null or trim(state) = '' then null 
            else state
        end as state::varchar(100),
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
        case 
            when neighborhood is null or trim(neighborhood) = '' then null 
            else neighborhood
        end as neighborhood::varchar(100),
        case 
            when councildistrictcode is null then null 
            else councildistrictcode 
        end as council_district_code
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
),

stg_location as (
    select
        {{ dbt_utils.generate_surrogate_key(['address', 'zipcode']) }} as id_location,
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
