{{
  config(
      materialized='table',
      on_schema_change='fail'
  )
}}

with src_property as (
    select
        {{ dbt_utils.generate_surrogate_key(['epa_property_type', 'property_gfa_total']) }} as id_Property,
        propertyname::varchar(100) as property_name,
        epapropertytype::varchar(100) as epa_property_type,
        numberofbuildings::int as number_of_buildings,
        propertygfatotal::float as property_gfa_total,
        propertygfaparking::float as property_gfa_parking,
        taxparcelidentificationnumber::int as tax_parcel_identification_number
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
),

stg_property as (
    select
        id_property,
        property_name,
        epa_property_type,
        number_of_buildings,
        property_gfa_total,
        property_gfa_parking,
        tax_parcel_identification_number
    from src_property
)

select * from stg_property