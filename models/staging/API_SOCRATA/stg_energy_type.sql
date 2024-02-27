/*
    Staging Model: stg_energy_type

    This dbt model stages raw data for energy types.
*/

{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

with energy_types as (
    select 'electricity' as energy_type union all
    select 'steam' union all
    select 'natural_gas'
),

stg_energy_type as (
    select
          {{ dbt_utils.generate_surrogate_key(['energy_type']) }} as id_energy_type
        , energy_type
    from energy_types
)

select * from stg_energy_type