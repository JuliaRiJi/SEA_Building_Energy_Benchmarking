/*
    Staging Model: stg_building

    This dbt model stages raw energy data from the buildings in Seattle
*/

{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

with src_building as (
    select
          osebuildingid::varchar(100) as ose_building_id
        , case
            when taxparcelidentificationnumber is null 
            then null
            else taxparcelidentificationnumber
          end as id_property
        , taxparcelidentificationnumber::varchar(100) as tax_parcel_identification_number
        , datayear::int as data_year
        , buildingname::varchar(100) as building_name
        , buildingtype::varchar(100) as building_type
        , address::varchar(100) as address
        , case 
            when zipcode is null then null 
            else zipcode::int
          end as zipcode
        , numberoffloors::int as number_of_floors
        , yearbuilt::date as year_built
        , totalghgemissions::float as total_ghg_emissions
        , ghgemissionsintensity::float as ghg_emissions_intensity
        , case 
            when energystarscore = 'NA' then null 
            when try_to_number(energystarscore) is null then null 
            else energystarscore::int
          end as energy_star_score
        , siteenergyuse_kbtu::float as site_energy_use_kbtu
        , siteenergyusewn_kbtu::float as site_energy_use_wn_kbtu
        , compliancestatus::varchar(100) as compliance_status
        , complianceissue::varchar(100) as compliance_issue
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where
    zipcode >= 0
    and site_energy_use_kbtu >= 0
    and site_energy_use_wn_kbtu >= 0
    and energy_star_score >= 0
),

stg_building as (
    select
          {{ dbt_utils.generate_surrogate_key(['ose_building_id']) }} as id_building
        , ose_building_id
        ,  {{ dbt_utils.generate_surrogate_key(['id_property']) }} as id_property
        , case 
            when address is not null and zipcode is not null 
            then {{ dbt_utils.generate_surrogate_key(['address', 'zipcode']) }} 
            else null 
          end as id_location
        , building_name
        , {{ dbt_utils.generate_surrogate_key(['building_type']) }} as id_building_type
        , number_of_floors
        , year_built
        , {{ dbt_utils.generate_surrogate_key(['data_year']) }} as id_data_year
        , data_year
        , total_ghg_emissions
        , ghg_emissions_intensity
        , energy_star_score
        , site_energy_use_kbtu
        , site_energy_use_wn_kbtu
        , compliance_status
        , compliance_issue
    from src_building
)

select * from stg_building

