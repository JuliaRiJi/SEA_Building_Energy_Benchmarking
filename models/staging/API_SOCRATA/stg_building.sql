/*
    Staging Model: stg_building_energy_benchmarking

    This dbt model stages raw data ---------------------.

*/

{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

with src_building as (
    select
        , osebuildingid
        , buildingname::varchar(100) as building_name
        , propertygfabuilding_s::float as property_gfa_buildings
        , numberoffloors::int as number_of_floors
        , yearbuilt::date as year_built
        , totalghemissions::float as total_gh_emissions
        , ghemissionsintensity::float as gh_emissions_intensity
        , energystarscore::int as energy_star_score
        , siteenergyuse_kbtu::float as site_energy_use_kbtu
        , siteenergyusewn_kbtu::float as site_energy_use_wn_kbtu
        , compliancestatus::varchar(100) as compliance_status
        , complianceissue::varchar(100) as compliance_issue
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    ),

with stg_building as (
    select
          {{ dbt_utils.generate_surrogate_key(['ose_building_id']) }} as id_building
        , ose_building_id
        , building_name
        , property_gfa_buildings
        , number_of_floors
        , year_built
        , total_gh_emissions
        , gh_emissions_intensity
        , energy_star_score
        , site_energy_use_kbtu
        , site_energy_use_wn_kbtu
        , compliance_status
        , compliance_issue
    from src_building
)

select * from stg_building
