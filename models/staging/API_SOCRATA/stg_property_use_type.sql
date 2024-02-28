/*
    Staging Model: stg_property_use_type

    This dbt model stages raw data of property use types.
*/

{{
  config(
      materialized='table',
      on_schema_change='fail'
  )
}}

with distinct_property_types as (
    select
        case
             when largestpropertyusetype = 'NA' then null
             else trim(largestpropertyusetype)::varchar(100) 
        end as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where largestpropertyusetype is not null and largestpropertyusetype != ''

    union all

    select
        case
            when secondlargestpropertyusetype = 'NA' then null
            else trim(secondlargestpropertyusetype)::varchar(100) 
        end as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where secondlargestpropertyusetype is not null and secondlargestpropertyusetype != ''

    union all

    select
        case 
            when thirdlargestpropertyusetype = 'NA' then null
            else trim(thirdlargestpropertyusetype)::varchar(100) 
        end as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where thirdlargestpropertyusetype is not null and thirdlargestpropertyusetype != ''

    union all
        
    select
        case
            when epapropertytype = 'NA' then null
            else trim(epapropertytype)::varchar(100) 
        end as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where epapropertytype is not null and epapropertytype != ''

    union all
        
    select
        case
            when primarypropertytype = 'NA' then null
            else trim(primarypropertytype)::varchar(100)
        end as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }}
    where primarypropertytype is not null and primarypropertytype != ''  

    union all

    select
        case
            when value = 'NA' then null
            else trim(value)::varchar(100)
        end as property_use_type
    from {{ source('api_socrata', 'building_energy_benchmarking') }},
    lateral split_to_table(listofallpropertyusetypes, ',') as split
    where split.value is not null and trim(split.value) <> ''
),

stg_property_use_type as (
    select
        {{ dbt_utils.generate_surrogate_key(['property_use_type']) }} as id_property_use_type
        , property_use_type
    from distinct_property_types
)

select distinct * from stg_property_use_type
