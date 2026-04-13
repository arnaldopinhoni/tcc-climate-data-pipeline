with latest_ingestions as (
    select
        city,
        bronze_record_id
    from (
        select
            city,
            bronze_record_id,
            row_number() over (
                partition by city
                order by ingestion_time desc, bronze_record_id desc
            ) as rn
        from (
            select distinct
                city,
                bronze_record_id,
                ingestion_time
            from {{ ref('silver_climate_hourly_history') }}
        ) as history_runs
    ) as ranked_ingestions
    where rn = 1
)

select
    history.bronze_record_id,
    history.city,
    history.ingestion_time,
    history.created_at,
    history.record_time,
    history.temperature_2m,
    history.relative_humidity_2m,
    history.precipitation,
    history.dew_point_2m,
    history.shortwave_radiation,
    history.wind_speed_10m,
    history.vapour_pressure_deficit,
    history.et0_fao_evapotranspiration
from {{ ref('silver_climate_hourly_history') }} as history
inner join latest_ingestions
    on history.city = latest_ingestions.city
   and history.bronze_record_id = latest_ingestions.bronze_record_id
