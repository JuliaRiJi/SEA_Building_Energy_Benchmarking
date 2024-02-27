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

    union all

    select distinct
        secondlargestpropertyusetype as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }}

    union all

    select distinct
        thirdlargestpropertyusetype as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
),

stg_property_use_type as (
    select
        {{ dbt_utils.generate_surrogate_key(['property_use_type']) }} as id_property_use_type
        , property_use_type
    from distinct_property_types
)

select * from stg_property_use_type;
