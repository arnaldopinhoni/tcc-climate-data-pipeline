select
    city,
    record_time,
    count(*) as total_rows
from {{ ref('silver_climate_hourly') }}
group by 1, 2
having count(*) > 1