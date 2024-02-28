/*
    Dimension table: dim_building_type

    This dbt dimension table ...........
*/

{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

with dim_building_type as (
    select *
    from {{ ref('stg_building_type') }}
)

select * from dim_building_type