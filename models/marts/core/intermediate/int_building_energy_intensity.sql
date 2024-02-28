
{{
  config(
    materialized='ephemeral'
  )
}}

with int_building_energy_intensity as (
    select 
        bei.id_building,
        bei.id_data_year,
        max(case when eui.energy_source = 'site' then bei.id_energy_use_intensity end) as id_site_energy_intensity,
        max(case when eui.energy_source = 'site' then bei.kbtu_value end) as site_kbtu_value,
        max(case when eui.energy_source = 'site' then bei.kbtu_value_wn end) as site_kbtu_value_wn,
        max(case when eui.energy_source = 'source' then bei.id_energy_use_intensity end) as id_source_energy_intensity,
        max(case when eui.energy_source = 'source' then bei.kbtu_value end) as source_kbtu_value,
        max(case when eui.energy_source = 'source' then bei.kbtu_value_wn end) as source_kbtu_value_wn
    from {{ ref('stg_building_energy_intensity') }} bei
    join {{ ref('stg_energy_use_intensity') }} eui
    on bei.id_energy_use_intensity = eui.id_energy_use_intensity
    group by 
        bei.id_building,
        bei.id_data_year
)

select * from int_building_energy_intensity
