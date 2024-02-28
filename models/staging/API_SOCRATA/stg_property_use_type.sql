{{
  config(
      materialized='table',
      on_schema_change='fail'
  )
}}

with distinct_property_types as (
    select distinct
        largestpropertyusetype as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where largestpropertyusetype is not null and largestpropertyusetype != ''

    union all

    select distinct
        secondlargestpropertyusetype as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where secondlargestpropertyusetype is not null and secondlargestpropertyusetype != ''

    union all

    select distinct
        thirdlargestpropertyusetype as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where thirdlargestpropertyusetype is not null and thirdlargestpropertyusetype != ''

    union all
        
    select distinct
        epapropertytype as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where epapropertytype is not null and epapropertytype != ''

    union all
        
    select 
        primarypropertytype as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where primarypropertytype is not null and primarypropertytype != ''  

    union all

    select distinct
        trim(value) as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }},
    lateral split_to_table(listofallpropertyusetypes, ',') as split
    where split.value is not null and trim(split.value) <> ''
),

stg_property_use_type as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['property_use_type']) }} as id_property_use_type
        , property_use_type
    from distinct_property_types
)

select * from stg_property_use_type
