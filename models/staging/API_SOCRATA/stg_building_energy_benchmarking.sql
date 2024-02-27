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

with src_building_energy_benchmarking as (
    select
          osebuildingid
        , datayear
        -- Agrega aquí las otras columnas necesarias
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    ),

stg_building_energy_benchmarking as (
    select
           osebuildingid
         , datayear
         -- Agrega aquí las transformaciones necesarias para las otras columnas
    from src_building_energy_benchmarking
    )

select * from stg_building_energy_benchmarking
