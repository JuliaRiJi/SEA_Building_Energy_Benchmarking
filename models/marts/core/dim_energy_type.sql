/*
    Dimension table: dim_energy_type

    This dbt dimension table ...........
*/

{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

with dim_energy_type as (
    select *
    from {{ ref('stg_energy_type') }}
)

select * from dim_energy_type