
{{
  config(
    materialized='ephemeral'
  )
}}

with int_building_energy_intensity as (
    select 
        bei.id_building,
        bei.data_year,
        max(case when bei.energy_source = 'site' then bei.id_energy_use_intensity end) as id_site_energy_intensity,
        max(case when bei.energy_source = 'site' then bei.kbtu_value end) as site_kbtu_value,
        max(case when bei.energy_source = 'site' then bei.kbtu_value_wn end) as site_kbtu_value_wn,
        max(case when bei.energy_source = 'source' then bei.id_energy_use_intensity end) as id_source_energy_intensity,
        max(case when bei.energy_source = 'source' then bei.kbtu_value end) as source_kbtu_value,
        max(case when bei.energy_source = 'source' then bei.kbtu_value_wn end) as source_kbtu_value_wn
from 
    {{ ref('stg_building_energy_intensity') }} bei
group by 
    bei.id_building,
    bei.data_year
)

select * from int_building_energy_intensity