{{
  config(
      materialized='table'
    , on_schema_change='fail'
  )
}}

with energy_sources as (
    select 'site' as energy_source, 
    'Site Energy Use is the annual amount of all the energy consumed by the property on-site, as reported on utility bills' as energy_desc
    union all
    select 'source', 
    'Source Energy Use is the annual energy used to operate the property, including losses from generation, transmission, & distribution' as energy_desc
),

stg_energy_use_intensity as (
    select
          {{ dbt_utils.generate_surrogate_key(['energy_source']) }} as id_energy_use_intensity
        , energy_source
        , energy_desc
    from energy_sources
)

select * from stg_energy_use_intensity
