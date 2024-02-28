/*
    Dimension table: dim_date

    This dbt dimension table ...........
*/

{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

{{ dbt_date.get_date_dimension("2015-01-01", "2023-01-01") }}