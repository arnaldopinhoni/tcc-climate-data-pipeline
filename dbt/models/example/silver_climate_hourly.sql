with bronze_ranked as (
    select
        city,
        raw_json,
        row_number() over (partition by city order by id desc) as rn
    from bronze_climate_raw
),

bronze_latest as (
    select
        city,
        raw_json
    from bronze_ranked
    where rn = 1
),

expanded as (
    select
        city,
        (raw_json->'hourly'->'time')::jsonb as record_times,
        (raw_json->'hourly'->'temperature_2m')::jsonb as temps,
        (raw_json->'hourly'->'relative_humidity_2m')::jsonb as hums,
        (raw_json->'hourly'->'precipitation')::jsonb as precs
    from bronze_latest
),

unnested as (
    select
        city,
        record_times->>i as record_time,
        temps->>i as temperature_2m,
        hums->>i as relative_humidity_2m,
        precs->>i as precipitation
    from expanded,
    generate_series(0, jsonb_array_length(record_times) - 1) as i
)

select
    city,
    record_time::timestamp as record_time,
    temperature_2m::numeric as temperature_2m,
    relative_humidity_2m::numeric as relative_humidity_2m,
    precipitation::numeric as precipitation
from unnested