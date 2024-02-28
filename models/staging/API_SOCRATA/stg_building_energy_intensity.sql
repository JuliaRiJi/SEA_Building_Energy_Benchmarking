{{
  config(
      materialized='table',
      on_schema_change='fail'
  )
}}

-- CTE for "site" energy
with site_energy_cte as (
    select
        {{ dbt_utils.generate_surrogate_key(['osebuildingid']) }} as id_building,
        'site' as energy_source,
        {{ dbt_utils.generate_surrogate_key(['energy_source']) }} as id_energy_use_intensity,
        datayear as data_year,
        siteeui_kbtu_sf as kbtu_value,
        siteeuiwn_kbtu_sf as kbtu_value_wn
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
),

-- CTE for "source" energy
with source_energy_cte as (
    select
        {{ dbt_utils.generate_surrogate_key(['osebuildingid']) }} as id_building,
        'source' as energy_source,
        {{ dbt_utils.generate_surrogate_key(['energy_source']) }} as id_energy_use_intensity,
        datayear as data_year,
        sourceeui_kbtu_sf as kbtu_value,
        sourceeuiwn_kbtu_sf as kbtu_value_wn
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
),

stg_building_energy_intensity as (
    select
        id_building,
        id_energy_use_intensity,
        data_year,
        kbtu_value,
        kbtu_value_wn,
    from site_energy_cte
    
    union all
    
    select
        id_building,
        id_energy_use_intensity,
        data_year,
        kbtu_value,
        kbtu_value_wn,
    from source_energy_wn_cte
)

select * from stg_building_energy_intensity