-- Tabela 2. Volume de registros por camada da arquitetura.
--
-- Na janela 14/03 a 01/04/2026, com 17 rodadas por cidade e 5 cidades:
--   Bronze              85 = 17 x 5
--   Silver historica    14.280 = 17 x 168 x 5
--   Silver corrente     840 = 168 x 5
--   Silver deduplicada  3.000 = 600 horas x 5 (25 datas-alvo x 24 h)
--   Gold historica      595 = 17 x 7 x 5
--   Gold corrente       35 = 7 x 5
--   Gold deduplicada    125 = 25 datas-alvo x 5
--
-- As camadas correntes nao levam filtro de janela: por definicao expoem a rodada mais
-- recente existente no banco, seja ela do periodo do estudo ou nao.

{% set janela %}
    ingestion_time::date between date '2026-03-14' and date '2026-04-01'
{% endset %}

select 1 as ordem, 'Bronze' as camada, 'Respostas brutas da API' as descricao,
       count(*) as total, count(*) / count(distinct city) as por_cidade
from {{ source('raw', 'bronze_climate_raw') }} where {{ janela }}

union all
select 2, 'Silver historica', 'Registros horarios expandidos',
       count(*), count(*) / count(distinct city)
from {{ ref('silver_climate_hourly_history') }} where {{ janela }}

union all
select 3, 'Silver corrente', 'Registros da ultima rodada',
       count(*), count(*) / count(distinct city)
from {{ ref('silver_climate_hourly') }}

union all
select 4, 'Silver deduplicada', 'Um registro por cidade e horario',
       count(*), count(*) / count(distinct city)
from ( {{ silver_dedup_janela() }} ) sd

union all
select 5, 'Gold historica', 'Agregacoes diarias por rodada',
       count(*), count(*) / count(distinct city)
from {{ ref('gold_daily_summary_history') }} where {{ janela }}

union all
select 6, 'Gold corrente', 'Agregacoes da ultima rodada',
       count(*), count(*) / count(distinct city)
from {{ ref('gold_daily_summary') }}

union all
select 7, 'Gold deduplicada', 'Uma previsao por cidade e data-alvo',
       count(*), count(*) / count(distinct city)
from ( {{ gold_dedup_janela() }} ) gd

order by ordem
