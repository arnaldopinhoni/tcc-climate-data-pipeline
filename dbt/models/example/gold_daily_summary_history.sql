select
    city,
    bronze_record_id,
    ingestion_time,
    date(record_time) as day,
    avg(temperature_2m) as avg_temp,
    max(temperature_2m) as max_temp,
    sum(precipitation) as total_precipitation,
    avg(dew_point_2m) as avg_dew_point_2m,
    avg(shortwave_radiation) as avg_shortwave_radiation,
    avg(wind_speed_10m) as avg_wind_speed_10m,
    avg(vapour_pressure_deficit) as avg_vapour_pressure_deficit,
    sum(et0_fao_evapotranspiration) as total_et0_fao_evapotranspiration
from {{ ref('silver_climate_hourly_history') }}
group by 1, 2, 3, 4
order by 3 desc, 4 desc, 1
