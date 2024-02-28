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
        , pur1.id_property_use_type as largest_property_use_id
        , pur1.property_use_type_gfa as largest_property_use_gfa
        , pur1.id_property_use_type as id_largest_property_use
        , pur2.id_property_use_type as second_property_use_id
        , pur2.property_use_type_gfa as second_property_use_gfa
        , pur2.id_property_use_type as id_second_property_use
        , pur3.id_property_use_type as third_property_use_id
        , pur3.property_use_type_gfa as third_property_use_gfa
        , pur3.id_property_use_type as id_third_property_use
    from {{ ref('stg_property') }} p
    join {{ ref('stg_property_use_type') }} put
    on p.epa_property_type = put.property_use_type
    left join {{ ref('stg_property_use_ranking') }} pur1
    on p.id_property = pur1.id_property and pur1.property_use_ranking = 1
    left join {{ ref('stg_property_use_ranking') }} pur2
    on p.id_property = pur2.id_property and pur2.property_use_ranking = 2
    left join {{ ref('stg_property_use_ranking') }} pur3
    on p.id_property = pur3.id_property and pur3.property_use_ranking = 3
)

select * from dim_property

