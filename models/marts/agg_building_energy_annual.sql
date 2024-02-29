{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

with fct_building_energy as (
    select
          id_building
        , id_data_year
        , site_energy_use_kbtu
    from {{ ref('fct_building_energy') }} 
),

dim_building as (
    select 
          id_building
        , building_name
    from {{ ref('dim_building') }}
),

/*dim_building_type as (
    select 
        id_building_type,
        building_type
    from {{ ref('dim_building_type') }}
),*/

dim_date as (
    select
        id_data_year
    from {{ ref('dim_date') }}
),

agg_building_energy_annual as (
    select
        fbe.id_building,
        db.building_name,
        site_energy_use_kbtu as annual_energy_consumption
from fct_building_energy fbe
inner join dim_building db 
on fbe.id_building = db.id_building
)

select * from agg_building_energy_annual