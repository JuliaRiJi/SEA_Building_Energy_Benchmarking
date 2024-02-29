/*
    Dimension table: dim_property_use_type

    This dbt dimension table ...........
*/

{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

with dim_property_use_type as (
    select *
    from {{ ref('stg_property_use_type') }}
)

select * from dim_property_use_type