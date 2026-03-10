select
    city,
    date(record_time) as day,
    avg(temperature_2m) as avg_temp,
    max(temperature_2m) as max_temp,
    sum(precipitation) as total_precipitation
from {{ ref('silver_climate_hourly') }}
group by 1, 2
order by 2 desc, 1