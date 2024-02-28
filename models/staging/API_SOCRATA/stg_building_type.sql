/*
    Staging Model: stg_building_type

    This dbt model stages raw building type data.
*/

{{
  config(
      materialized='table',
      on_schema_change='fail'
  )
}}

with src_building_type as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['buildingtype']) }} as id_building_type,
        buildingtype as building_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
),

stg_building_type as (
    select
        id_building_type,
        building_type
    from src_building_type
)

select * from stg_building_type
