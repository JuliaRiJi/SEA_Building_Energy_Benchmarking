/*
    Staging Model: stg_building_energy_type

    This dbt model stages raw energy type data used by buildings in Seattle.
*/

{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

-- CTE para la energía eléctrica
with electricity_cte as (
    select
        {{ dbt_utils.generate_surrogate_key(['osebuildingid']) }} as id_building
        , 'electricity' as energy_type
        , {{ dbt_utils.generate_surrogate_key(['energy_type']) }} as id_energy_type
        , datayear as data_year
        , electricity_kbtu as kbtu_value
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
),

-- CTE para la energía de gas natural
natural_gas_cte as (
    select
        {{ dbt_utils.generate_surrogate_key(['osebuildingid']) }} as id_building
        , 'natural_gas' as energy_type
        , {{ dbt_utils.generate_surrogate_key(['energy_type']) }} as id_energy_type
        , datayear as data_year
        , naturalgas_kbtu as kbtu_value
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
),

-- CTE para la energía de vapor
steam_cte as (
    select
        {{ dbt_utils.generate_surrogate_key(['osebuildingid']) }} as id_building
        , 'steam' as energy_type
        , {{ dbt_utils.generate_surrogate_key(['energy_type']) }} as id_energy_type
        , datayear as data_year
        , steamuse_kbtu as kbtu_value
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
),

-- Unión de todos los tipos de energía y selección de los campos deseados
stg_building_energy_type as (
    select
        id_building,
        id_energy_type,
        {{ dbt_utils.generate_surrogate_key(['data_year']) }} as id_data_year,
        kbtu_value
    from electricity_cte
    
    union all
    
    select
        id_building,
        id_energy_type,
        {{ dbt_utils.generate_surrogate_key(['data_year']) }} as id_data_year,
        kbtu_value
    from natural_gas_cte
    
    union all
    
    select
        id_building,
        id_energy_type,
        data_year,
        kbtu_value
    from steam_cte
)

select * from stg_building_energy_type
