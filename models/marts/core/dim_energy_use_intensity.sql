/*
    Dimension table: dim_energy_use_intensity

    This dbt dimension table ...........
*/

{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

with dim_energy_use_intensity as (
    select *
    from {{ ref('stg_energy_use_intensity') }}
)

select * from dim_energy_use_intensity