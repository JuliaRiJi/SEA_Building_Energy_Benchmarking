/*
    Fact table: fct_building_energy

    This dbt fact table ...........
*/

{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

with fct_building_energy as (
    select
          b.id_building
        , b.id_building_type
        , b.id_property
        , b.id_data_year
        , b.total_ghg_emissions
        , b.ghg_emissions_intensity
        , b.energy_star_score
        , b.site_energy_use_kbtu
        , b.site_energy_use_wn_kbtu
        , b.compliance_status
        , b.compliance_issue
        , b.id_location
        , bei.id_site_energy_intensity
        , bei.site_kbtu_value
        , bei.site_kbtu_value_wn
        , bei.id_source_energy_intensity
        , bei.source_kbtu_value
        , bei.source_kbtu_value_wn
        , bet.id_energy_electricity
        , bet.id_energy_steam
        , bet.id_energy_natural_gas
        , bet.electricity_kbtu
        , bet.steam_kbtu
        , bet.natural_gas_kbtu
    from {{ ref('stg_building') }} b
    left join {{ ref('int_building_energy_intensity') }} bei
    on b.id_building = bei.id_building and b.id_data_year = bei.id_data_year
    left join {{ ref('int_building_energy_intensity') }} bet
    on b.id_building = bet.id_building and b.id_data_year = bet.id_data_year
)

select * from fct_building_energy