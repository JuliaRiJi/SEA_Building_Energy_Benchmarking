/*
    Dimension table: dim_building

    This dbt dimension table ...........
*/

{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

with dim_building as (
    select 
          id_building
        , building_name
        , year_built
        , number_of_floors
    from {{ ref('stg_building') }}
)

select * from dim_building