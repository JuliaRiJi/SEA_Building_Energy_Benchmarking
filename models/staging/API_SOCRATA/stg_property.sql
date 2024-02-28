/*
    Staging Model: stg_property

    This dbt model stages raw property data about properties in Seattle.
*/

{{
  config(
      materialized='table',
      on_schema_change='fail'
  )
}}

with src_property as (
    select
        case
            when taxparcelidentificationnumber is null 
            then null
            else {{ dbt_utils.generate_surrogate_key(['taxparcelidentificationnumber']) }}
        end as id_property
        , taxparcelidentificationnumber::varchar(100) as tax_parcel_identification_number
        , datayear::date as data_year
        , case 
            when propertyname is null or propertyname = '' then 'unknown' 
            else propertyname 
          end as property_name
        , case 
            when epapropertytype is null or epapropertytype = '' then 'unknown' 
            else epapropertytype 
          end as epa_property_type
        , case 
            when primarypropertytype is null or primarypropertytype = '' then 'unknown' 
            else primarypropertytype 
          end as primary_property_type
        , case 
            when epa_building_sub_type_name is null or epa_building_sub_type_name = '' then 'unknown' 
            else epa_building_sub_type_name 
          end as epa_building_sub_type_name
        , numberofbuildings::int as number_of_buildings
        , propertygfatotal::float as property_gfa_total
        , propertygfaparking::float as property_gfa_parking
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
),

stg_property as (
    select
        id_property,
        tax_parcel_identification_number,
        property_name::varchar(100),
        epa_property_type::varchar(100),
        primary_property_type::varchar(100),
        epa_building_sub_type_name::varchar(100),
        number_of_buildings,
        property_gfa_total,
        property_gfa_parking
    from src_property
)

select * from stg_property