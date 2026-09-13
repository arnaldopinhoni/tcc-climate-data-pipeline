-- Base analitica para caracterizacao meteorologica.
--
-- A camada gold historica guarda uma linha por (cidade, rodada de ingestao, data-alvo),
-- de modo que uma mesma data do calendario aparece tantas vezes quantas rodadas a
-- previram. Somar precipitacao ou evapotranspiracao sobre aquela camada nao produz o
-- acumulado do periodo: produz a soma de previsoes repetidas para os mesmos dias.
--
-- Este modelo resolve isso mantendo UMA previsao por cidade e por data-alvo, escolhida
-- como a da rodada mais recente que cobriu aquela data (menor antecedencia disponivel).
-- E esta a base que deve ser usada para caracterizar o periodo meteorologico.
-- A camada *_history permanece para estudar a evolucao das previsoes entre rodadas.
--
-- ATENCAO ao reproduzir os numeros do trabalho. Este modelo deduplica sobre todo o
-- historico disponivel, que segue crescendo depois do fim da coleta do estudo. Para
-- recortar uma janela, filtre por ingestion_time ANTES de deduplicar, nao depois:
-- use a macro gold_dedup_janela (dbt/macros/janela_estudo.sql), que faz nessa ordem.

select distinct on (city, day)
    city,
    bronze_record_id,
    ingestion_time,
    day,
    (day - ingestion_time::date) as lead_days,
    avg_temp,
    max_temp,
    min_temp,
    total_precipitation,
    avg_dew_point_2m,
    avg_shortwave_radiation,
    avg_wind_speed_10m,
    avg_vapour_pressure_deficit,
    total_et0_fao_evapotranspiration
from {{ ref('gold_daily_summary_history') }}
order by city, day, ingestion_time desc, bronze_record_id desc
