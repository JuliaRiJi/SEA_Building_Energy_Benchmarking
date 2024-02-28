/*
    Staging Model: stg_property_use_ranking

    This dbt model stages raw data to rank property use types.
*/

{{
  config(
      materialized='table',
      on_schema_change='fail'
  )
}}

with src_property_use_ranking as (

    select
        datayear as data_year,
        trim(taxparcelidentificationnumber) as tax_parcel_identification_number,
        {{ dbt_utils.generate_surrogate_key(['tax_parcel_identification_number']) }} as id_property,
        trim(largestpropertyusetype) as largest_property_use_type,
        {{ dbt_utils.generate_surrogate_key(['largest_property_use_type']) }} as id_property_use_type,
        1 as property_use_ranking,
        largestpropertyusetypegfa as property_use_type_gfa
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where largestpropertyusetype is not null and trim(largestpropertyusetype) <> ''

    union all

    select
        datayear as data_year,
        trim(taxparcelidentificationnumber) as tax_parcel_identification_number,
        {{ dbt_utils.generate_surrogate_key(['tax_parcel_identification_number']) }} as id_property,
        trim(secondlargestpropertyusetype) as second_largest_property_use_type,
        {{ dbt_utils.generate_surrogate_key(['second_largest_property_use_type']) }} as id_property_use_type,
        2 as property_use_ranking,
        secondlargestpropertyuse as property_use_type_gfa
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where secondlargestpropertyusetype is not null and trim(secondlargestpropertyusetype) <> ''

    union all

    select
        datayear as data_year,
        trim(taxparcelidentificationnumber) as tax_parcel_identification_number,
        {{ dbt_utils.generate_surrogate_key(['tax_parcel_identification_number']) }} as id_property,
        trim(thirdlargestpropertyusetype) as third_largest_property_use_type,
        {{ dbt_utils.generate_surrogate_key(['third_largest_property_use_type']) }} as id_property_use_type,
        3 as property_use_ranking,
        thirdlargestpropertyusetypegfa as property_use_type_gfa
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where thirdlargestpropertyusetype is not null and trim(thirdlargestpropertyusetype) <> ''
),

stg_property_use_ranking as (
    select * from src_property_use_ranking
)

select * from stg_property_use_ranking