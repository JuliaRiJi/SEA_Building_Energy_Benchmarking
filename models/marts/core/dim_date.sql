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

with dim_date_generate as (
    {{ dbt_date.get_date_dimension("2015-01-01", "2023-01-01") }}
),

dim_date as (
    select distinct
        year_number
    from dim_date_generate
)

select 
    {{ dbt_utils.generate_surrogate_key(['year_number']) }} as id_data_year,
    year_number
from dim_date
order by year_number desc