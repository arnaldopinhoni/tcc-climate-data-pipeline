-- Equivalente horario de gold_daily_summary_dedup.
--
-- Mantem UM registro por cidade e por horario, o da rodada mais recente que previu
-- aquele horario. Necessario para as variaveis que nao sao agregadas na camada gold,
-- como a umidade relativa, e para estatisticas de dispersao no grao horario.
--
-- Mesma regra de desempate de silver_climate_hourly: ingestion_time desc e, em caso
-- de empate exato (reexecucoes no mesmo instante), bronze_record_id desc.
--
-- ATENCAO ao reproduzir os numeros do trabalho: para recortar uma janela de coleta,
-- filtre por ingestion_time ANTES de deduplicar. Ver a macro silver_dedup_janela.

select distinct on (city, record_time)
    bronze_record_id,
    city,
    ingestion_time,
    created_at,
    record_time,
    (record_time::date - ingestion_time::date) as lead_days,
    temperature_2m,
    relative_humidity_2m,
    precipitation,
    dew_point_2m,
    shortwave_radiation,
    wind_speed_10m,
    vapour_pressure_deficit,
    et0_fao_evapotranspiration
from {{ ref('silver_climate_hourly_history') }}
order by city, record_time, ingestion_time desc, bronze_record_id desc
