-- Cada rodada de ingestao deve devolver exatamente 168 registros horarios
-- (7 dias x 24 h) e cobrir exatamente 7 datas-alvo distintas.
--
-- O horizonte de 7 dias e o padrao do endpoint /v1/forecast da Open-Meteo e agora
-- e fixado explicitamente em etl/utils/api_client.py (forecast_days=7). Este teste
-- falha se a resposta da API mudar de tamanho, o que antes passaria despercebido e
-- alteraria em silencio todas as contagens por camada.

select
    bronze_record_id,
    city,
    count(*) as total_horas,
    count(distinct date(record_time)) as total_dias
from {{ ref('silver_climate_hourly_history') }}
group by 1, 2
having count(*) <> 168
    or count(distinct date(record_time)) <> 7
