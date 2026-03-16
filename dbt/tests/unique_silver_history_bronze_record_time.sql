select
    bronze_record_id,
    record_time,
    count(*) as total_rows
from {{ ref('silver_climate_hourly_history') }}
group by 1, 2
having count(*) > 1
