/*
    Dimension table: dim_location

    This dbt dimension table ...........
*/

{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

with dim_location as (
    select *
    from {{ ref('stg_location') }}
)

select * from dim_location