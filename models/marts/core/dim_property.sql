/*
    Dimension table: dim_property

    This dbt dimension table ...........
*/

{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

with dim_property as (
    select
          p.id_property
        , p.data_year
        , p.property_name
        , p.number_of_buildings
        , p.property_gfa_buildings
        , p.property_gfa_total
        , p.property_gfa_parking
        , put.id_property_use_type as id_epa_property_type
        , max(case when pur.property_use_ranking = 1 then put.property_use_type end) as largest_property_use
        , max(case when pur.property_use_ranking = 1 then pur.property_use_type_gfa end) as largest_property_use_gfa
        , max(case when pur.property_use_ranking = 1 then pur.id_property_use_type end) as id_largest_property_use
        , max(case when pur.property_use_ranking = 2 then put.property_use_type end) as second_property_use
        , max(case when pur.property_use_ranking = 2 then pur.property_use_type_gfa end) as second_property_use_gfa
        , max(case when pur.property_use_ranking = 2 then pur.id_property_use_type end) as id_second_property_use
        , max(case when pur.property_use_ranking = 3 then put.property_use_type end) as third_property_use
        , max(case when pur.property_use_ranking = 3 then pur.property_use_type_gfa end) as third_property_use_gfa
        , max(case when pur.property_use_ranking = 3 then pur.id_property_use_type end) as id_third_property_use
    from {{ ref('stg_property') }} p
    join {{ ref('stg_property_use_ranking') }} pur
    on p.id_property = pur.id_property
    left join {{ ref('stg_property_use_type') }} put
    on pur.id_property_use_type = put.id_property_use_type
    group by
          p.id_property
        , p.data_year
        , p.property_name
        , p.number_of_buildings
        , p.property_gfa_buildings
        , p.property_gfa_total
        , p.property_gfa_parking
        , put.id_property_use_type
)
select * from dim_property
