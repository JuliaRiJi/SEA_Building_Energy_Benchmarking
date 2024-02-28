/*
    Staging Model: stg_property_use_ranking

    This dbt model stages raw data to rank property use types.
*/

{{
  config(
      materialized='table',
      on_schema_change='fail'
  )
}}

with src_property_use_ranking as (

    select
        datayear as data_year,
        case
            when taxparcelidentificationnumber is null 
            then null
            else taxparcelidentificationnumber
        end as id_property,
        trim(largestpropertyusetype)::varchar(100) as property_use_type,
        1 as property_use_ranking,
        case 
            when largestpropertyusetypegfa = 'NA' then null 
            when try_to_number(largestpropertyusetypegfa) is null then null 
            else largestpropertyusetypegfa::float
        end as property_use_type_gfa,
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where largestpropertyusetype is not null and trim(largestpropertyusetype) <> ''

    union all

    select
        datayear as data_year,
        case
            when taxparcelidentificationnumber is null 
            then null
            else taxparcelidentificationnumber
        end as id_property,
        trim(secondlargestpropertyusetype)::varchar(100) as property_use_type,
        2 as property_use_ranking,
        case 
            when secondlargestpropertyuse = 'NA' then null 
            when try_to_number(secondlargestpropertyuse) is null then null 
            else secondlargestpropertyuse::float
        end as property_use_type_gfa,
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where secondlargestpropertyusetype is not null and trim(secondlargestpropertyusetype) <> ''

    union all

    select
        datayear as data_year,
        case
            when taxparcelidentificationnumber is null 
            then null
            else taxparcelidentificationnumber
        end as id_property,
        trim(thirdlargestpropertyusetype)::varchar(100) as property_use_type,
        3 as property_use_ranking,
        case 
            when thirdlargestpropertyusetypegfa = 'NA' then null 
            when try_to_number(thirdlargestpropertyusetypegfa) is null then null 
            else thirdlargestpropertyusetypegfa::float
        end as property_use_type_gfa
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where thirdlargestpropertyusetype is not null and trim(thirdlargestpropertyusetype) <> ''
),

stg_property_use_ranking as (
    select 
        data_year,
        {{ dbt_utils.generate_surrogate_key(['id_property']) }} as id_property,
        {{ dbt_utils.generate_surrogate_key(['property_use_type']) }} as id_property_use_type,
        property_use_ranking,
        property_use_type_gfa
     from src_property_use_ranking
)

select * from stg_property_use_ranking