{{
  config(
      materialized='table',
      on_schema_change='fail'
  )
}}

with src_property_use_ranking as (

    select
        datayear as data_year,
        {{ dbt_utils.generate_surrogate_key(['taxparcelidentificationnumber']) }} as id_property,
        {{ dbt_utils.generate_surrogate_key([trim(largestpropertyusetype)]) }} as id_property_use_type,
        1 as property_use_ranking,
        largestpropertyusetypegfa as property_use_type_gfa
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where largestpropertyusetype is not null and trim(largestpropertyusetype) <> ''

    union all

    select
        datayear as data_year,
        {{ dbt_utils.generate_surrogate_key(['taxparcelidentificationnumber']) }} as id_property,
        {{ dbt_utils.generate_surrogate_key([trim(secondlargestpropertyusetype)]) }} as id_property_use_type,
        2 as property_use_ranking,
        secondlargestpropertyuse as property_use_type_gfa
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where secondlargestpropertyusetype is not null and trim(secondlargestpropertyusetype) <> ''

    union all

    select
        datayear as data_year,
        {{ dbt_utils.generate_surrogate_key(['taxparcelidentificationnumber']) }} as id_property,
        {{ dbt_utils.generate_surrogate_key([trim(thirdlargestpropertyusetype)]) }} as id_property_use_type,
        3 as property_use_ranking,
        thirdlargestpropertyusetypegfa as property_use_type_gfa
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where thirdlargestpropertyusetype is not null and trim(thirdlargestpropertyusetype) <> ''

    union all

    select
        datayear as data_year,
        {{ dbt_utils.generate_surrogate_key(['taxparcelidentificationnumber']) }} as id_property,
        {{ dbt_utils.generate_surrogate_key(['value']) }} as id_property_use_type,
        '>3' as property_use_ranking,
        null as property_use_type_gfa
    from (
        select 
            datayear as data_year,
            taxparcelidentificationnumber,
            trim(value) as value,
            row_number() over (partition by datayear, taxparcelidentificationnumber order by datayear, taxparcelidentificationnumber) 
            as row_num,
        from {{ source('api_socrata', 'building_energy_benchmarking') }},
        lateral split_to_table(listofallpropertyusetypes, ',') as split
        where split.value is not null and trim(split.value) <> ''
    ) split_list
    where row_num > 3
      and trim(value) not in (largestpropertyusetype, secondlargestpropertyusetype, thirdlargestpropertyusetype)
),

stg_property_use_ranking as (
    select * from src_property_use_ranking
)

select * from stg_property_use_ranking