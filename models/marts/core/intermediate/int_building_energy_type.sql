{{
  config(
    materialized='ephemeral'
  )
}}

with int_building_energy_type as (
    select 
        bet.id_building,
        bet.id_data_year,
        max(case when et.energy_type = 'electricity' then bet.id_energy_type end) as id_energy_electricity,
        max(case when et.energy_type = 'electricity' then bet.kbtu_value end) as electricity_kbtu,
        max(case when et.energy_type = 'steam' then bet.id_energy_type end) as id_energy_steam,
        max(case when et.energy_type = 'steam' then bet.kbtu_value end) as steam_kbtu,
        max(case when et.energy_type = 'natural_gas' then bet.id_energy_type end) as id_energy_natural_gas,
        max(case when et.energy_type = 'natural_gas' then bet.kbtu_value end) as natural_gas_kbtu
    from 
        {{ ref('stg_building_energy_type') }} bet
    join 
        {{ ref('stg_energy_type') }} et
    on 
        bet.id_energy_type = et.id_energy_type
    group by 
        bet.id_building,
        bet.id_data_year
)

select * from int_building_energy_type