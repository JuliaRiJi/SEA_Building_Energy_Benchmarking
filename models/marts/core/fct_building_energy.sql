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

with building_energy as (
    select
          b.id_building
        , b.id_building_type
        , b.id_property
        , bei.id_energy_use_intensity as id_site_energy_intensity
        , bei.kbtu_value as site_kbtu_value
        , bei.kbtu_value_wn as site_kbtu_value_wn

        , b.total_ghg_emissions
        , b.ghg_emissions_intensity
        , b.energy_star_score
        , b.site_energy_use_kbtu
        , b.site_energy_use_wn_kbtu
        , b.compliance_status
        , b.compliance_issue
    from {{ ref('stg_building') }} b
    join {{ ref('stg_building_type') }} bt
    on b.id_building_type = bt.id_building_type
    join {{ ref('stg_property') }} p 
    on b.id_property=p.id_property
    join {{ ref('stg_building_energy_intensity') }} bei
    on b.id_building=bei.id_building
    where bei.energy_source = 'site'

    union
    
    select
          bei.id_energy_use_intensity as id_source_energy_intensity
        , bei.kbtu_value as source_kbtu_value
        , bei.kbtu_value_wn as source_kbtu_value_wn
    from {{ ref('stg_building') }} b
    join {{ ref('stg_building_energy_intensity') }} bei
    on b.id_building=bei.id_building
    where bei.energy_source = 'source'
)