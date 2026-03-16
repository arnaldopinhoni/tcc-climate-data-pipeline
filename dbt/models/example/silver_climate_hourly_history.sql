with expanded as (
    select
        id as bronze_record_id,
        city,
        ingestion_time,
        created_at,
        (raw_json->'hourly'->'time')::jsonb as record_times,
        (raw_json->'hourly'->'temperature_2m')::jsonb as temps,
        (raw_json->'hourly'->'relative_humidity_2m')::jsonb as hums,
        (raw_json->'hourly'->'precipitation')::jsonb as precs,
        (raw_json->'hourly'->'dew_point_2m')::jsonb as dew_points,
        (raw_json->'hourly'->'shortwave_radiation')::jsonb as shortwave_radiations,
        (raw_json->'hourly'->'wind_speed_10m')::jsonb as wind_speeds_10m,
        (raw_json->'hourly'->'vapour_pressure_deficit')::jsonb as vapour_pressure_deficits,
        (raw_json->'hourly'->'et0_fao_evapotranspiration')::jsonb as et0_fao_values
    from bronze_climate_raw
),

unnested as (
    select
        bronze_record_id,
        city,
        ingestion_time,
        created_at,
        record_times->>i as record_time,
        temps->>i as temperature_2m,
        hums->>i as relative_humidity_2m,
        precs->>i as precipitation,
        dew_points->>i as dew_point_2m,
        shortwave_radiations->>i as shortwave_radiation,
        wind_speeds_10m->>i as wind_speed_10m,
        vapour_pressure_deficits->>i as vapour_pressure_deficit,
        et0_fao_values->>i as et0_fao_evapotranspiration
    from expanded,
    generate_series(0, jsonb_array_length(record_times) - 1) as i
)

select
    bronze_record_id,
    city,
    ingestion_time,
    created_at,
    record_time::timestamp as record_time,
    temperature_2m::numeric as temperature_2m,
    relative_humidity_2m::numeric as relative_humidity_2m,
    precipitation::numeric as precipitation,
    dew_point_2m::numeric as dew_point_2m,
    shortwave_radiation::numeric as shortwave_radiation,
    wind_speed_10m::numeric as wind_speed_10m,
    vapour_pressure_deficit::numeric as vapour_pressure_deficit,
    et0_fao_evapotranspiration::numeric as et0_fao_evapotranspiration
from unnested
